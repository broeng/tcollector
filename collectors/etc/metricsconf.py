
# Should the collector even attempt to collect?
enabled = True

# Metrics endpoint
endpoint = "http://127.0.0.1:8082/metrics"

# How often should we collect?
poll_interval = 60

# If the first max attempts at collecting fail,
# we abort entirely. Later failures are ignored.
max_attempts  = 15

# Prefix for collected metrics
prefix = "metrics"

# Exclusion list for collected tag values
tagv_exclusions = [ ]
