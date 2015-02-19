#!/usr/bin/env python

#
# WORK IN PROGRESS!
#

import os.path
import re
import sys
import time

from collectors.lib import utils
from collectors.lib import httpagentparser

# Register a few additional browsers with the httpagentparser
class Monit(httpagentparser.Browser):
    look_for = "monit"
    version_markers = [("/", "")]
    bot = True

class Curl(httpagentparser.Browser):
    look_for = "curl"
    version_markers = [("/", "")]
    allow_space_in_version = False
    bot = True

class CCBot(httpagentparser.Browser):
    """ http://commoncrawl.org/faq/ """
    look_for = "CCBot"
    version_markers = [("/", "")]
    allow_space_in_version = False
    bot = True

for browser in [Monit, Curl, CCBot]:
    httpagentparser.detectorshub.register(browser())

# Load configuration
state_file   = "/tmp/.useragent-collector.state"
regexp_str   = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'
name_mapping = {}
log_errors   = True

try:
    def confOr(name, default):
        return getattr(conf, name) if hasattr(conf, name) else default
    from collectors.etc import useragentconf as conf
    log_file       = conf.log_file
    state_file     = confOr('state_file', state_file)
    log_errors     = confOr('log_errors', log_errors)
    regexp_str     = confOr('regexp_str', regexp_str)
    name_mapping   = confOr('name_mapping', name_mapping)
    ua_blacklist   = confOr('useragent_blacklist', [])
    path_whitelist = confOr('path_whitelist', [])
except ImportError:
    log_file = None

# Log parser, and precompiled regexp
regexp = re.compile(regexp_str)

class Format:
    """Formatting operations for reporting to OpenTSDB"""
    @staticmethod
    def _no_whitespace(v):
        return "NA" if v is None else v.replace(" ", "")
    @staticmethod
    def name(v):
        v = Format._no_whitespace(v)
        return v if v not in name_mapping else name_mapping[v]
    @staticmethod
    def version(v):
        return Format._no_whitespace(v)

class Timestamp:
    """Load and store state"""
    @staticmethod
    def read():
        last_processed = int(time.time())
        if os.path.exists(state_file):
            with open(state_file) as f:
                return int(f.readline())
        return last_processed
    @staticmethod
    def write(ts):
        with open(state_file, "w") as f:
            f.write(str(ts))

def parse_datetime(dt):
    # Split datetime string and tz offset
    dtstr, tzoffset = tuple(dt.split(" "))
    # Parse the date time string ignoring TZ data for now
    return time.strptime(dtstr, "%d/%b/%Y:%H:%M:%S")

class ParseResult:
    def __init__(self, result):
        self.result = result
FAILED  = ParseResult(None)
IGNORED = ParseResult(None)

# Representation of a client request
class UserAgentDetails:
    """A parsed HTTP client request"""
    def __init__(self, date_time, source_ip, is_bot=False, user_agent=None, os=None):
        def orNA(d, k):
            return "NA" if k not in d else d[k]
        # Prepare details dictionaries
        user_agent = user_agent if user_agent is not None else {}
        os         = os if os is not None else {}
        # Extract User-Agent specific info
        self.ua_name    = orNA(user_agent, "name")
        self.ua_version = orNA(user_agent, "version")
        self.is_bot     = is_bot
        # Extract OS specific info
        self.os_name    = orNA(os, "name")
        self.os_version = orNA(os, "version")
        # Set timestamp for request
        self.date_time  = int(time.mktime(date_time))
        # source ip
        self.source_ip  = source_ip

def parse_log_line(line):
    """Parse supplied log line in apache access log format"""
    def orEmpty(d, k):
        return {} if k not in d else d[k]
    result = regexp.match(line)
    if result is None:
        return FAILED
    groups = result.groups()
    if len(groups) < 7:
        return FAILED
    # Extract groups from log line
    src, dt, req, status_code, sz, referrer, user_agent = groups
    # Get request path
    request_parts = req.split(" ")
    if len(request_parts) != 3:
        return FAILED
    method, path, version = tuple(request_parts)
    if path not in path_whitelist:
        return IGNORED
    # Determine browser and OS
    details = httpagentparser.detect(user_agent, fill_none=False)
    return ParseResult(
        UserAgentDetails(
            parse_datetime(dt),
            src,
            is_bot = details["bot"] if "bot" in details else False,
            user_agent = orEmpty(details, "browser"),
            os = orEmpty(details, "os" if "os" in details else "platform")))

def main():
    # Make sure a log_file has been specified, and that it exists
    if log_file is None:
        utils.err("No log_file defined in useragentconf.py")
        return -13
    if not os.path.exists(log_file):
        utils.err("Log file not found: "+log_file)
        return -13
    # Determine start offset for logged entries.
    last_processed = Timestamp.read()
    # Start monitoring log file
    while True:
        try:
            with open(log_file) as f:
                for line in iter(f.readline, ''):
                    res = parse_log_line(line)
                    # Ensure we were able to parse the log entry
                    if res is FAILED:
                        if log_errors: utils.err("Failed to parse line: "+line)
                        continue
                    # Make sure the entry was not actively filtered out.
                    if res is IGNORED: continue
                    ua = res.result
                    # Skip any bots in the reporting
                    if ua.is_bot: continue
                    # Make sure, the entry is not before our cut-off ts
                    if ua.date_time < last_processed: continue
                    # Update last_processed ts
                    last_processed = ua.date_time
                    # Register the metric
                    print "useragent.request %d %s os_name=%s os_version=%s ua_name=%s ua_version=%s src=%s" % (
                        ua.date_time,
                        "1",
                        Format.name(ua.os_name),
                        Format.version(ua.os_version),
                        Format.name(ua.ua_name),
                        Format.version(ua.ua_version),
                        ua.source_ip)
            sys.stdout.flush()
            Timestamp.write(last_processed)
        except Exception as e:
            utils.err("Caught error, exiting: %s" % e)
            return -13
        time.sleep(10)

if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main())
