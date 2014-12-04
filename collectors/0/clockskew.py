#!/usr/bin/env python
import sys
import socket
import time

from collectors.lib import utils

try:
    import ntplib
except ImportError:
    ntplib = None

try:
    from collectors.etc import clockskewconf as conf
    ntp_host = conf.ntp_host
except ImportError:
    ntp_host = "pool.ntp.org"

def get_offset():
    # connect to ntp server
    client = ntplib.NTPClient()
    reply  = client.request(ntp_host, version=3)
    return reply.offset

def err(msg):
    utils.err(msg)

def metric(m, v):
    return "clock.%s %d %s ntphost=%s" % (
        m, int(time.time()), abs(v), ntp_host)

def main():
    failures = 0
    if ntplib is None:
        return -13
    while True:
        try:
            print metric("failed_queries", failures)
            print metric("skew", get_offset())
            sys.stdout.flush()
        except socket.gaierror as e:
            err("Caught error: %s" % e)
            failures += 1
        except ntplib.NTPException as e:
            err("Caught NTPException: %s" % e)
            failures += 1
        except Exception as e:
            err("Caught error, exiting: %s" % e)
            return -13
        time.sleep(180)

if (__name__ == "__main__"):
    sys.stdin.close()
    sys.exit(main())
