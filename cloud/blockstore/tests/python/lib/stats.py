from __future__ import print_function

import requests


def apply_filters(stat_filter, labels):
    if stat_filter is None:
        return False
    if isinstance(stat_filter, str):
        return labels.find(stat_filter) >= 0
    for f in stat_filter:
        res = apply_filters(f, labels)
        if res:
            return True
    return False


def dump_stats(url, stat_filter, out, debug_out):
    r = requests.get(url, timeout=10)
    r.raise_for_status()

    j = r.json()
    sensors = j["sensors"]
    for sensor in sensors:
        label_map = sensor["labels"]
        labels = "/".join(sorted(
            ["%s:%s" % (x, y) for x, y in label_map.items()]))
        if apply_filters(stat_filter, labels) is False:
            continue

        value = sensor["value"]
        if "histogram" not in label_map:
            print("%s=%s" % (labels, value != 0), file=out)
        print("%s=%s" % (labels, value), file=debug_out)
