#!/usr/bin/env python
"""
    Collector for monit instances
    
    Monit: https://mmonit.com/monit/

    Requires read access to the administration HTTP 
    interface. By default, the collector will attempt 
    to connect to the URL:

        http://127.0.0.1:2812

    If you need to change this, you can override the 
    default in /etc/default/tcollector, set

        MONIT_URL=http://user:pass@host:port

    Example configuration, compatible with default 
    no-auth monit URL:

        set httpd port 2812 and
            use address localhost
            allow localhost 

    TODO:
        - Report events
        - Report groups (tho, a metric must have same 
          set of labels, and labels are single-value)

    Feedback: sbj@cwconsult.dk

"""

import sys
import time
import urllib
import xml.etree.ElementTree as ET

from collectors.etc import monitconf
from collectors.lib import utils

# load settings from config module
settings = monitconf.settings()
enabled       = settings["enabled"]
poll_interval = settings["poll_interval"] # seconds
max_attempts  = settings["max_attempts"]
monit_url     = settings["monit_url"]

#
# Utility method
#
def val(node, path):
    n = node.find(path) if node is not None else None
    return n.text if n is not None else None

def float_val(node, path):
    v = val(node, path)
    return float(v) if v is not None else None

def now():
    return int(time.time())

#
# Custom URLOpener
#
class NonPromptingURLOpener(urllib.FancyURLopener):
    """Disables prompting for Basic AUTH credentials, when not in URL"""
    def prompt_user_passwd(self, host, realm):
        utils.err("monit server requires authentication, supply in MONIT_URL")
        return ("", "")

class PollError(Exception):
    pass

#
# Collectors for hostname, services, and system
#
class Host:
    """Collect common host information from reporting"""
    def __init__(self, root):
        self.version = root.get('version')
        self.host    = val(root, './server/localhostname')
        self.coll_ts = val(root, './services/service[0]/collected_sec')
    def tags(self):
        """Build map of host-specific tags"""
        return { }
    def metric(self, name, value, tags=None):
        """Format a metric for reporting"""
        # Ensure we have a value
        if value is None: return None
        tags = {} if tags is None else tags
        # combine dicts with tags
        tags.update(self.tags())
        # Format map of tags to string
        fmt_tags = ["%s=%s" % (k,v) for k,v in tags.items() if v is not None]
        # Return formatted metric line
        return "monit.%s %s %s %s" % (
                name, now(), value, " ".join(fmt_tags))

class Services:
    """Collect aggregated information from available services"""
    types = {
        0: "FS", 1: "DIR", 2: "FILE", 3: "PROCESS", 4: "HOST",
        5: "SYSTEM", 6: "FIFO", 7: "PROGRAM"
    }
    monitor = { 0: "OFF", 1: "ON", 2: "INIT", 3: "WAITING" }
    mode = { 0: "ACTIVE", 1: "PASSIVE", 2: "MANUAL" }
    def __init__(self, root):
        # filters for finding failed and ok services
        f_ok   = lambda n: n.find("status").text == "0"
        f_fail = lambda n: not f_ok(n)
        f_mon  = lambda n: n.find("monitor").text == "1"
        f_unmon= lambda n: not f_mon(n)
        # find all service nodes with status and monitor children
        s_services = root.findall('./services/service[status]')
        m_services = root.findall('./services/service[monitor]')
        # count ok and failed services
        self.ok     = len(filter(f_ok, s_services))
        self.failed = len(filter(f_fail, s_services))
        # count monitored services
        self.monitored   = len(filter(f_mon, m_services))
        self.unmonitored = len(filter(f_unmon, m_services))
    @staticmethod
    def name(types, tcode):
        return types[int(tcode)] if int(tcode) in types else None
    @staticmethod
    def services(host, root):
        for service in root.findall('./services/service'):
            # Extract service information
            service_name= service.get('name')
            typecode    = val(service, './type')
            status      = val(service, './status')
            # build map of tags common for service
            tags = {
                'service': service_name,
                'type': Services.name(Services.types, typecode),
                'status': status
            }
            # build service status metric
            yield host.metric("service.status", status, tags)
            # Get extra metrics related to HOST checks
            for port in service.findall('./port[responsetime]'):
                # Get responsetime, and convert it to ms
                resptime_ms = int(float_val(port, './responsetime') * 1000.0);
                # Get tags specific to this type
                port_tags = {
                    'service': service_name,
                    'status': status,
                    'endpoint': val(port, './hostname'),
                    'portnumber': val(port, './portnumber'),
                    'reqtype': val(port, './type')
                }
                # build metric for reponsetime and separate metric for host-status
                yield host.metric("service.host.status", status, port_tags)
                # only report responsetime if available
                if (resptime_ms > 0):
                    yield host.metric("service.host.responsetime", resptime_ms, port_tags)

class System:
    """Collect system related information"""
    def __init__(self, root):
        # find system node
        system = root.find('.//service/system')
        # load avgs
        self.avg01 = val(system, './load/avg01')
        self.avg05 = val(system, './load/avg05')
        self.avg15 = val(system, './load/avg15')
        # cpu info
        self.cpu_user = val(system, './cpu/user')
        self.cpu_sys  = val(system, './cpu/system')
        self.cpu_wait = val(system, './cpu/wait')
        # memory info
        self.mem_percent = val(system, './memory/percent')
        self.mem_kb = val(system, './memory/kilobyte')
        # swap info
        self.swap_percent = val(system, './swap/percent')
        self.swap_kb = val(system, './swap/kilobyte')


def collect(reporting):
    """Collect and report available metrics"""
    def filter_metrics(fn, *args):
        for col in args:
            for e in filter(fn, col):
                yield e
    # Parse the reporting
    root = ET.fromstring(reporting)
    # extract information
    host     = Host(root)
    services = Services(root)
    sys      = System(root)
    service_metrics = Services.services(host, root)
    # Filter invalid entries and return result
    return filter_metrics(lambda s: s is not None, [
        host.metric("services.ok", services.ok),
        host.metric("services.failed", services.failed),
        host.metric("services.monitored", services.monitored),
        host.metric("services.unmonitored", services.unmonitored),
        host.metric("system.loadavg01", sys.avg01),
        host.metric("system.loadavg05", sys.avg05),
        host.metric("system.loadavg15", sys.avg15),
        host.metric("system.cpu.user", sys.cpu_user),
        host.metric("system.cpu.system", sys.cpu_sys),
        host.metric("system.cpu.wait", sys.cpu_wait),
        host.metric("system.memory.percent", sys.mem_percent),
        host.metric("system.memory.kbytes", sys.mem_kb),
        host.metric("system.swap.percent", sys.swap_percent),
        host.metric("system.swap.kbytes", sys.swap_kb)],
        service_metrics)

def poll_monit():
    # Query monit
    opener = NonPromptingURLOpener()
    resp   = opener.open("%s/_status2?format=xml" % monit_url)
    # Read response
    report = resp.read() if resp is not None else None
    status = resp.getcode()
    resp.close()
    # Raise error if we did not poll successfully
    if (status != 200):
        raise PollError("HTTP Request Status: %d" % status)
    return report

def main():
    # prepare statistics
    polls = 0
    failures = 0
    # Poll monit forever (unless it's unavailable..)
    while enabled:
        try:
            # Abort if we haven't been able to poll
            # monit instance in max_attempts.
            if polls == max_attempts and polls == failures:
                # Tell tcollector not to reschedule.
                return 13
            # Poll monit instance
            polls += 1
            report = poll_monit()
            # collect and print metrics
            for metric in collect(report):
                print metric
            sys.stdout.flush()
        except Exception as e:
            utils.err("Caught error: %s" % e)
            failures += 1
        # sleep until next poll
        time.sleep(poll_interval)

if (__name__ == "__main__"):
    sys.stdin.close()
    sys.exit(main())
