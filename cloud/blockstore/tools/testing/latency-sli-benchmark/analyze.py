#!/usr/bin/env python3
"""Audit a complete benchmark batch and export paired comparisons."""
import argparse
import csv
import json
from pathlib import Path
import random
import statistics

parser = argparse.ArgumentParser()
parser.add_argument('directory', type=Path)
args = parser.parse_args()
root = args.directory
metadata = json.loads((root/'metadata.json').read_text())
rows = [json.loads(line) for line in (root/'runs.jsonl').read_text().splitlines()]
repeats = metadata['parameters']['runs']
index = {(r['case'], r['variant'], r['round']): r for r in rows}
assert len(index) == len(rows), 'Duplicate measurement keys'
cases = sorted({r['case'] for r in rows})
assert len(rows) == len(cases)*3*repeats, 'Incomplete measurement batch'
assert metadata.get('finished'), 'The benchmark runner did not finish'
randomizer = random.Random(7885)
results = []
for case in cases:
    data = {v: [index[(case, v, i)] for i in range(repeats)]
            for v in ['main', 'off', 'on']}
    for r in data['on']:
        assert r.get('decision_checksum', 0) or r.get('latency_verified_good', 0), \
            'Enabled measurement did not verify SLI counters'
    def median(variant, field):
        return statistics.median(r[field] for r in data[variant])
    ratios = [data['on'][i]['ops_per_sec']/data['main'][i]['ops_per_sec']
              for i in range(repeats)]
    bootstrap = sorted(statistics.median(randomizer.choices(ratios, k=repeats))
                       for _ in range(10000))
    change = 100*(median('on', 'ops_per_sec')/median('main', 'ops_per_sec')-1)
    completion = 'completion_p50_us' if case.startswith('split') else 'p50_us'
    result = {
        'case': case,
        'main_ops_per_sec': median('main', 'ops_per_sec'),
        'off_ops_per_sec': median('off', 'ops_per_sec'),
        'on_ops_per_sec': median('on', 'ops_per_sec'),
        'throughput_change_percent': change,
        'paired_change_percent': 100*(statistics.median(ratios)-1),
        'paired_ci95_low_percent': 100*(bootstrap[250]-1),
        'paired_ci95_high_percent': 100*(bootstrap[9749]-1),
        'main_p50_us': median('main', 'p50_us'),
        'on_p50_us': median('on', 'p50_us'),
        'main_p99_us': median('main', 'p99_us'),
        'on_p99_us': median('on', 'p99_us'),
        'main_completion_p50_us': median('main', completion),
        'on_completion_p50_us': median('on', completion),
        'completion_p50_change_ns': 1000*(median('on', completion)-median('main', completion)),
        'main_cpu_us_per_op': statistics.median(r['cpu_us_per_op']+r['server_cpu_us_per_op'] for r in data['main']),
        'on_cpu_us_per_op': statistics.median(r['cpu_us_per_op']+r['server_cpu_us_per_op'] for r in data['on']),
        'passes_3_percent_limit': change >= -3,
    }
    results.append(result)
with (root/'comparison.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(results[0]))
    writer.writeheader()
    writer.writerows(results)
(root/'analysis.json').write_text(json.dumps(results, indent=2))
for r in results:
    print(f"{r['case']}: {r['throughput_change_percent']:+.2f}% throughput; "
          f"{r['main_completion_p50_us']:.3f} -> {r['on_completion_p50_us']:.3f} us completion p50; "
          f"paired 95% interval [{r['paired_ci95_low_percent']:+.2f}%, {r['paired_ci95_high_percent']:+.2f}%]")
print('All median throughput comparisons pass:', all(r['passes_3_percent_limit'] for r in results))
