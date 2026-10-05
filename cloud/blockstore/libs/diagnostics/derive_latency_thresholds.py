#!/usr/bin/env python3
"""Derive a review candidate from exported ExecutionTime histogram counts.

Input JSON: {source: 'ExecutionTime', period: {start: ISO-date, end: ISO-date},
 rows: [{media_kind: int, write: bool, start_bytes: int, end_bytes: int,
         service_path: str, buckets: [{upper_us: int|null, count: int}]}]}.
Periods are half-open. Counts are per bucket, NOT cumulative. Null denotes +Inf.
Rows use existing non-overlapping size ranges. Use distinct base/validation files.
This command neither queries monitoring nor deploys/enables a configuration.
"""
import argparse
import datetime as dt
import json
from pathlib import Path

MAX_U64 = (1 << 64) - 1


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, minimum=0, maximum=MAX_U64):
    require(type(value) is int and minimum <= value <= maximum, 'invalid integer')
    return value


def load(path):
    raw = Path(path).read_bytes()
    require(len(raw) <= 16 * 1024 * 1024, 'histogram export is too large')
    data = json.loads(raw)
    require(data['source'] == 'ExecutionTime', 'source must be ExecutionTime')
    start, end = (dt.date.fromisoformat(data['period'][key]) for key in ('start', 'end'))
    require(start < end, 'empty observation period')
    rows = {}
    for row in data['rows']:
        media = integer(row['media_kind'], maximum=(1 << 32) - 1)
        require(type(row['write']) is bool, 'write must be a boolean')
        lower, upper = integer(row['start_bytes']), integer(row['end_bytes'])
        require(lower < upper, 'empty size range')
        key = (media, row['write'], lower, upper)
        require(key not in rows, 'duplicate histogram; aggregate counts before export')
        require(isinstance(row['service_path'], str) and row['service_path'], 'service_path is required')
        previous, seen_inf = 0, False
        require(row['buckets'], 'empty histogram')
        for bucket in row['buckets']:
            bound = bucket['upper_us']
            integer(bucket['count'])
            require(not seen_inf, '+Inf must be last')
            if bound is None:
                seen_inf = True
            else:
                integer(bound, minimum=1, maximum=MAX_U64 - 1)
                require(bound > previous, 'bucket boundaries must increase')
                previous = bound
        require(seen_inf, 'include the +Inf bucket, even when its count is zero')
        rows[key] = row
    require(0 < len(rows) <= 256, 'expected 1..256 size ranges')
    keys = sorted(rows)
    for a, b in zip(keys, keys[1:]):
        require(a[:2] != b[:2] or a[3] <= b[2], 'overlapping size ranges')
    return (start, end), rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True)
    parser.add_argument('--validation', required=True)
    parser.add_argument('--version', required=True, type=int)
    parser.add_argument('--min-observations', required=True, type=int)
    parser.add_argument('--max-threshold-us', required=True, type=int,
                        help='reviewer-selected maximum acceptable candidate latency')
    parser.add_argument('--output', required=True, help='new JSON candidate/report file')
    args = parser.parse_args()
    integer(args.version, minimum=1, maximum=(1 << 32) - 1)
    integer(args.min_observations, minimum=1)
    integer(args.max_threshold_us, minimum=1, maximum=MAX_U64 - 1)
    base_period, base = load(args.base)
    validation_period, validation = load(args.validation)
    require(28 <= (base_period[1] - base_period[0]).days <= 30,
            'base history must cover 28..30 days')
    require(base_period[1] <= validation_period[0] or validation_period[1] <= base_period[0],
            'base and validation periods must be independent')
    require(base.keys() == validation.keys(), 'base and validation ranges must match')
    output = {'review_required': True, 'enabled': False, 'threshold_version': args.version,
              'base_period': list(map(str, base_period)),
              'validation_period': list(map(str, validation_period)), 'rows': []}
    config = ['EnableLatency: false', f'LatencyThresholdVersion: {args.version}']
    for key, row in sorted(base.items()):
        check = validation[key]
        require(row['service_path'] == check['service_path'], 'service paths must be comparable')
        require([b['upper_us'] for b in row['buckets']] == [b['upper_us'] for b in check['buckets']],
                'base and validation bucket boundaries must match')
        total = sum(b['count'] for b in row['buckets'])
        validation_total = sum(b['count'] for b in check['buckets'])
        require(min(total, validation_total) >= args.min_observations, f'insufficient observations: {key}')
        covered, threshold = 0, None
        for bucket in row['buckets']:
            covered += bucket['count']
            if covered * 1000 >= total * 999:
                threshold = bucket['upper_us']
                break
        require(threshold is not None, f'p99.9 falls in +Inf: {key}')
        require(threshold <= args.max_threshold_us, f'candidate exceeds reviewed ceiling: {key}')
        validation_covered = sum(b['count'] for b in check['buckets']
                                 if b['upper_us'] is not None and b['upper_us'] <= threshold)
        output['rows'].append(dict(zip(('media_kind', 'write', 'start_bytes', 'end_bytes'), key),
                                  service_path=row['service_path'], threshold_us=threshold,
                                  base_count=total, validation_count=validation_total,
                                  validation_covered=validation_covered,
                                  validation_fraction=validation_covered / validation_total))
        media, write, lower, upper = key
        config.append(f'LatencyThresholds {{ MediaKind: {media} Write: {str(write).lower()} '
                      f'StartBytes: {lower} EndBytes: {upper} ThresholdUs: {threshold} }}')
    output['candidate_textproto'] = '\n'.join(config) + '\n'
    # Require a new artifact; never overwrite an approved table accidentally.
    with open(args.output, 'x', encoding='utf-8') as stream:
        json.dump(output, stream, indent=2)
        stream.write('\n')


if __name__ == '__main__':
    main()
