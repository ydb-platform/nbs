#!/usr/bin/env python

import argparse
import requests
import sys
import time
from multiprocessing.pool import ThreadPool


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', help="Base url, example: http://ydbproxy-sas.ydb.cloud.yandex.net:8765")
    parser.add_argument('--token', help="IAM token")
    args = parser.parse_args()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            tablet_id = int(line)
            if tablet_id == 0:
                continue
        except ValueError:
            print(f'Failed to parse tablet id: {line}', file=sys.stderr)

    with ThreadPool(processes=args.inflight) as pool:
        pool.starmap(work, [(args.url, args.token, args.hive_tablet_id, tablet) for tablet in tablets])
