#!/usr/bin/env python

def defaults():
    defs = {}
    try:
        with open("/etc/default/tcollector","r") as f:
            for line in f.readlines():
                if line.startswith("#"): 
                    continue
                key, value = line.partition("=")[::2]
                defs[key] = value[:-1] 
    except Exception as e:
        pass
    return defs

def settings():
    def value(key, default, config=defaults()):
        """Get value for key, by checking in /etc/default/ first"""
        return config[key] if key in config else default
    # default monit url
    monit_url = "http://127.0.0.1:2812"
    return {
        "enabled": True,
        "poll_interval": 30,
        "max_attempts": 15,
        "monit_url": value("MONIT_URL", monit_url)
    }
