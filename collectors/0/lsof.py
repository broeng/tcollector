#!/usr/bin/env python

import time
import subprocess
import sys

from collectors.etc import lsofconf as conf

valid = sum([
    [chr(x) for x in range(ord('0'), ord('9')+1)],
    [chr(x) for x in range(ord('A'), ord('Z')+1)],
    [chr(x) for x in range(ord('a'), ord('z')+1)],
], ['_', '.', '/', '-'])

def sanitize(s):
    s = s.replace(' ', '_')
    return filter(lambda x: x in valid, s)

def now():
    return int(time.time())

def lsof():
    lines = subprocess.check_output(["lsof", "-F", "0ucnt"] + conf.filesystems).splitlines()

    fs = {}

    ts = now()

    n = 0

    line = lines[n]

    while n < len(lines):
        # split into parts
        parts = line.split('\0')[:-1]

        # extract pid and command
        pid, cmd, user = parts[0][1:], parts[1][1:], parts[2][1:]

        n += 1

        # now parse files
        while n < len(lines):
            line = lines[n]

            if line[0] == 'p':
                # we reached a new process
                break

            # otherwise, parse file
            if line[0] == 'f':
                parts = line.split('\0')[:-1]

                fd = parts[0][1:]
                typ = parts[1][1:]
                path = parts[2][1:]

                k = '{0}.openfiles {1} # cmd={2} pid={3} user={4} type={5} path={6}'.format(
                    conf.prefix, ts,
                    sanitize(cmd), pid, user, typ, sanitize(path),
                )

                if k in fs:
                    fs[k] += 1
                else:
                    fs[k] = 1

            n += 1

    for k, v in fs.iteritems():
        print k.replace("#", str(v))

def main():
    while conf.enabled:
        lsof()
        time.sleep(conf.poll_interval)


if __name__ == '__main__':
    sys.stdin.close()
    sys.exit(main())
