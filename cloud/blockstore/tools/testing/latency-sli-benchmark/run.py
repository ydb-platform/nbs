#!/usr/bin/env python3
"""Compare fresh main, feature-off and feature-on with identical harnesses."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import statistics
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--base', required=True)
p.add_argument('--feature', required=True)
p.add_argument('--output', required=True)
p.add_argument('--runs', type=int, default=9)
p.add_argument('--device-dir', help='Backing device directory; use tmpfs to isolate CPU overhead')
p.add_argument('--seconds', type=float, default=4)
p.add_argument('--no-sync', action='store_true', help='Benchmark AIO without O_SYNC')
p.add_argument('--mode', choices=['split', 'vhost', 'all'], default='all')
p.add_argument('--case', help='Run only the named scenario, for longer follow-up measurements')
a = p.parse_args()
all_cases = {f'split-{parts}-{op}' for parts in [1, 16, 128] for op in ['read', 'write']} | {f'vhost-{size}-qd{depth}' for size in [4096, 1048576] for depth in [1, 32]}
if a.case and (a.case not in all_cases or
               (a.mode != 'all' and not a.case.startswith(a.mode+'-'))):
    p.error('--case must name a scenario available in the selected mode')
out = Path(a.output)
out.mkdir(parents=True, exist_ok=True)
if (out/'runs.jsonl').exists():
    p.error('Use a new output directory to keep measurement batches separate')
bench = 'cloud/blockstore/tools/testing/latency-sli-benchmark/latency-sli-benchmark'
server = 'cloud/blockstore/vhost-server/blockstore-vhost-server'
variants = {'main': (Path(a.base), False), 'off': (Path(a.feature), False),
            'on': (Path(a.feature), True)}

def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1024*1024), b''):
            h.update(b)
    return h.hexdigest()

metadata = {'parameters': vars(a), 'started': time.time(),
            'cpu': subprocess.check_output(['lscpu'], text=True),
            'binaries': {name: {kind: digest(root / path)
                for kind, path in [('benchmark', bench), ('server', server)]}
                for name, (root, _) in variants.items()},
            'base_revision': 'bae11be0414c00bd4d473cc2afed64f0f12f010d',
            'acceptance': 'median enabled throughput >= 97% of fresh main for every case'}
(out/'metadata.json').write_text(json.dumps(metadata, indent=2))
bounds = [1, 4097, 65537, 1048577, 8388609, 1 << 40]
latency_config = '1' + ''.join(f';{write}:{start}:{end}:1000000000'
    for write in [0, 1] for start, end in zip(bounds, bounds[1:]))
rows = []
randomizer = random.Random(7885)

def run(cmd, timeout=45):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if r.returncode:
        raise RuntimeError(f'{cmd}: {r.returncode}\n{r.stderr[-4000:]}')
    return json.loads(r.stdout.strip().splitlines()[-1])

def record(row):
    row['recorded_at'] = time.time()
    row['loadavg'] = Path('/proc/loadavg').read_text().strip()
    row['cpu_stat'] = {line.split()[0]: list(map(int, line.split()[1:]))
        for line in Path('/proc/stat').read_text().splitlines()
        if line.split()[0] in ['cpu16', 'cpu17', 'cpu18', 'cpu19', 'cpu20']}
    rows.append(row)
    with (out/'runs.jsonl').open('a') as f:
        f.write(json.dumps(row)+'\n')
    print(json.dumps({k: row[k] for k in ['case', 'variant', 'round', 'ops_per_sec', 'p50_us', 'p99_us']}), flush=True)

if a.mode in ['split', 'all']:
    cases = [(parts, write) for parts in [1, 16, 128] for write in [0, 1]]
    if a.case:
        cases = [(parts, write) for parts, write in cases
                 if a.case == f'split-{parts}-'+('write' if write else 'read')]
    for iteration in range(a.runs):
        randomizer.shuffle(cases)
        for parts, write in cases:
            names = list(variants)
            randomizer.shuffle(names)
            for name in names:
                root, enabled = variants[name]
                row = run(['taskset', '-c', '20', str(root/bench), 'split',
                           str(int(enabled)), str(parts), str(a.seconds), str(write)])
                record(dict(row, case=f'split-{parts}-'+('write' if write else 'read'),
                            variant=name, round=iteration))

if a.mode in ['vhost', 'all']:
    # A 1 MiB operation crosses both devices; 4 KiB stays within one.
    devices = []
    device_dir = Path(a.device_dir) if a.device_dir else out
    device_dir.mkdir(parents=True, exist_ok=True)
    for index in range(2):
        path = device_dir/f'device-{index}.bin'
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.posix_fallocate(fd, 0, 512*1024)
        os.close(fd)
        devices += ['--device', f'{path}:524288:0']
    cases = [(size, depth) for size in [4096, 1048576] for depth in [1, 32]]
    if a.case:
        cases = [(size, depth) for size, depth in cases
                 if a.case == f'vhost-{size}-qd{depth}']
    for iteration in range(a.runs):
        randomizer.shuffle(cases)
        for size, depth in cases:
            names = list(variants)
            randomizer.shuffle(names)
            for name in names:
                root, enabled = variants[name]
                prefix = out/f'vhost-{size}-{depth}-{iteration}-{name}'
                socket = str(prefix)+'.sock'
                cmd = ['taskset', '-c', '16,17', str(root/server),
                       '--socket-path', socket, '--serial', 'sli-light-benchmark',
                       '--disk-id', 'sli-light-benchmark', '--block-size', '4096',
                       '--queue-count', '1', '--no-chmod', '--verbose', 'error'] + devices
                if a.no_sync:
                    cmd += ['--no-sync']
                if enabled:
                    cmd += ['--latency-sli', latency_config]
                with open(str(prefix)+'.out', 'w') as stdout, open(str(prefix)+'.err', 'w') as stderr:
                    proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
                    try:
                        deadline = time.monotonic()+10
                        while not os.path.exists(socket):
                            if proc.poll() is not None or time.monotonic() > deadline:
                                raise RuntimeError(f'vhost startup failed: {prefix}')
                            time.sleep(.05)
                        row = run(['taskset', '-c', '18,19', str(root/bench), 'vhost',
                                   socket, str(size), str(depth), str(a.seconds), '50', str(proc.pid)])
                        if enabled:
                            # The completion thread may be idle in io_getevents.
                            # Await a fresh snapshot instead of assuming a fixed delay.
                            snapshot = None
                            for attempt in range(3):
                                before = Path(str(prefix)+'.out').read_text().splitlines()
                                proc.send_signal(signal.SIGUSR1)
                                deadline = time.monotonic()+3
                                while time.monotonic() < deadline:
                                    lines = Path(str(prefix)+'.out').read_text().splitlines()
                                    if len(lines) > len(before):
                                        snapshot = json.loads(lines[-1])
                                        break
                                    time.sleep(.05)
                                if snapshot and snapshot['latency_sli']['fresh']:
                                    break
                            assert snapshot and snapshot['latency_sli']['fresh']
                            # Read-only validation of the new producer telemetry.
                            good = sum(snapshot[k]['latency_good'] for k in ['read', 'write'])
                            assert good >= row['operations'], (good, row)
                            assert all(snapshot[k]['latency_bad'] == 0 and
                                       snapshot[k]['latency_unknown'] == 0 for k in ['read', 'write'])
                            row['latency_verified_good'] = good
                        record(dict(row, case=f'vhost-{size}-qd{depth}',
                                    variant=name, round=iteration))
                    finally:
                        if proc.poll() is None:
                            proc.send_signal(signal.SIGINT)
                            try:
                                proc.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()

summary = []
for case in sorted({r['case'] for r in rows}):
    table = {}
    for name in variants:
        subset = [r for r in rows if r['case'] == case and r['variant'] == name]
        table[name] = {key: statistics.median(r[key] for r in subset)
            for key in ['ops_per_sec', 'p50_us', 'p99_us', 'ns_per_op', 'cpu_us_per_op']
            if all(key in r for r in subset)}
    delta = 100*(table['on']['ops_per_sec']/table['main']['ops_per_sec']-1)
    summary.append({'case': case, 'medians': table, 'throughput_change_percent': delta,
                    'passes_3_percent_limit': delta >= -3})
(out/'summary.json').write_text(json.dumps(summary, indent=2))
metadata['finished'] = time.time()
(out/'metadata.json').write_text(json.dumps(metadata, indent=2))
print(json.dumps(summary, indent=2), flush=True)
