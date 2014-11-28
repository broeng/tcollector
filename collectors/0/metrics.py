#!/usr/bin/env python

import sys
import time
import urllib
import json

from collectors.etc import metricsconf as conf
from collectors.lib import utils

# Min value reported
min_value = 0.00000001

# Exclusion substrings for tag values, only really
# relevant for the path tag
tagv_exclusions = conf.tagv_exclusions
# metric key exclusion list
metric_exclusions = [ "units" ]

#
# Utility methods
#
def excluded(s, exclusions):
    return any(map(lambda ex: ex in s, exclusions))

def metric(name, value, **tags):
    """Format a metric for reporting"""
    def escape_tagv(v):
        if v == "*": return "SUM"
        return v.replace("****", "_")
    # Ensure we have a value
    if value is None:
        return None
    if type(value) not in [float, int]:
        err('Excluding metric %s, not int/float value: %s (tags: %s)' %
            (name, value, tags))
        return None
    if float(value) < min_value:
        value = min_value
    # Validate tags, skip invalids
    for k,v in tags.items():
        if " " in v: 
            return None
        if excluded(v, tagv_exclusions):
            return None
    # Format map of tags to string
    fmt_tags = ["%s=%s" % (k, escape_tagv(v)) for k,v in tags.items() if v is not None]
    # Return formatted metric line
    return "%s.%s %s %s %s" % (
        conf.prefix, name, now(), 
        '{0:.8f}'.format(value), 
        " ".join(fmt_tags))

def now():
    return int(time.time())

def err(s):
    utils.err(s)

#
# Collectors for timers, meters, gauges and histograms
#
def handle_metrics(node, path, component):
    class MetricType:
        def noop(node, **kws):
            return []
        def _report(coll, metric_type=None, exclude_list=None):
            """Build metrics from @metrics categories"""
            exclusions = metric_exclusions if exclude_list is None else exclude_list
            for collection, kvs in coll.items():
                if not hasattr(kvs, "items"):
                    continue
                for k, v in kvs.items():
                    if not excluded(k, exclusions):
                        yield metric(k, v,
                            metric_name=collection,
                            metric_type=metric_type,
                            path=path,
                            module=component)
        timers     = _report
        meters     = _report
        gauges     = _report
        histograms = _report
        counters   = _report
        version    = noop
    # handle each of the groups in @metrics objects
    for group, children in node.items():
        # Boo! Hisss! ... but it's so convenient...
        metrics = vars(MetricType)[group](children, metric_type=group)
        for m in metrics:
            yield m

def descend(node, path=None, component=None):
    if node is None: return
    for key, children in node.items():
        if key == "@metrics":
            for m in handle_metrics(children, path, component):
                yield m
        else:
            name = key if path is None else "%s/%s" % (path, key)
            comp = key if component is None else component
            for m in descend(children, name, comp):
                yield m

def collect(reporting):
    """Collect and report available metrics"""
    def filter_metrics(fn, *args):
        for col in args:
            for e in filter(fn, col):
                yield e
    # Parse the reporting
    root = json.loads(reporting)
    # Filter invalid entries and return result
    return filter_metrics(lambda s: s is not None, descend(root))

def poll_metrics():
    resp   = urllib.urlopen(conf.endpoint)
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
    # Poll metrics endpoint forever (unless it's unavailable..)
    while conf.enabled:
        try:
            # Abort if we haven't been able to poll
            # metrics instance in max_attempts.
            if polls == conf.max_attempts and polls == failures:
                # Tell tcollector not to reschedule.
                return 13
            # Poll instance
            polls += 1
            report = poll_metrics()
            # collect and print metrics
            for metric in collect(report):
                print metric
            sys.stdout.flush()
        except Exception as e:
            err("Caught error (%s): %s" % (conf.endpoint,e))
            failures += 1
        # sleep until next poll
        time.sleep(conf.poll_interval)

if (__name__ == "__main__"):
    sys.stdin.close()
    sys.exit(main())
