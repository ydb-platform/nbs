#!/usr/bin/env python

from datetime import datetime, timedelta

import sys

EPOCH = datetime.utcfromtimestamp(0)
BLOCK_SIZE = 4096
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DISCR = 100000  # 100000us, 100ms
US_IN_SECOND = 1000000

data = []
prev_ts = None

for line in sys.stdin.readlines():
    parts = line.rstrip().split("\t")
    dt = datetime.strptime(parts[0][:-1], DATE_FORMAT)
    ts = int((dt - EPOCH).total_seconds() * 1000000)

    blocks = 0
    ranges = parts[6].split(" ")
    for r in ranges:
        index, count = r.split(",")
        blocks += int(count)

    data.append((ts, BLOCK_SIZE * blocks))
    prev_ts = ts

max_mbps = 0
max_iops = 0

data.sort(key=lambda x: x[0])

last_start_ts = None
last_end_ts = None
byte_count = 0
total_byte_count = 0
request_count = 0
total_request_count = 0

for p in data:
    ts = p[0]

    if last_start_ts is None:
        last_start_ts = ts
        last_end_ts = ts

    if ts > last_start_ts + DISCR:
        mb_count = byte_count / 1024 / 1024
        ts_diff = max(DISCR, last_end_ts - last_start_ts)
        cur_mbps = mb_count * US_IN_SECOND / ts_diff
        cur_iops = request_count * US_IN_SECOND / ts_diff
        if cur_mbps > max_mbps:
            max_mbps = cur_mbps
        if cur_iops > max_iops:
            max_iops = cur_iops
        dt = datetime.fromtimestamp(last_start_ts / 1000000) + timedelta(microseconds=last_start_ts % 1000000)
        dtstr = dt.strftime(DATE_FORMAT)
        print >> sys.stdout, "ts=%s, bytes=%s" % (dtstr, byte_count)
        print >> sys.stdout, "ts=%s, request_count: %s" % (dtstr, request_count)
        byte_count = 0
        request_count = 0
        last_start_ts = ts

    byte_count += p[1]
    total_byte_count += p[1]
    request_count += 1
    total_request_count += 1
    last_end_ts = ts

print >> sys.stderr, "MAX MiB/s: %s" % max_mbps
print >> sys.stderr, "AVG MiB/s: %s" % (total_byte_count * 1000000 / 1024 / 1024 / (data[-1][0] - data[0][0]))
print >> sys.stderr, "MAX IOPS: %s" % max_iops
print >> sys.stderr, "AVG IOPS: %s" % (total_request_count / (data[-1][0] - data[0][0]))
