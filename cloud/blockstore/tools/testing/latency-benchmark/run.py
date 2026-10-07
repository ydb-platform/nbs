#!/usr/bin/env python3
"""Repeated release-mode latency instrumentation benchmarks. No product edits."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import select
import signal
import statistics
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]
SERVER_CPUS = '16,17'
CLIENT_CPUS = '18,19'
COMPONENT_CPU = '20'


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout


def proc_stats(pid):
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
        fields = text[text.rfind(')') + 2:].split()
        return {
            'cpu_seconds': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
            'rss_kib': int(fields[21]) * os.sysconf('SC_PAGE_SIZE') / 1024,
        }
    except (FileNotFoundError, ProcessLookupError):
        return None


def cpu_snapshot():
    return {f[0]: [int(v) for v in f[1:]] for line in Path('/proc/stat').read_text().splitlines()
            if (f := line.split()) and (f[0] == 'cpu' or f[0].startswith('cpu') and f[0][3:].isdigit())}


def background():
    lines = command(['ps', '-eo', 'comm,args']).splitlines()
    return {
        'loadavg': [float(v) for v in Path('/proc/loadavg').read_text().split()[:3]],
        'compiler_processes': sum(line.split(maxsplit=1)[0] in ('clang', 'clang++') for line in lines if line.split()),
    }


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def run_client(args, log_path):
    rss = []
    before = cpu_snapshot()
    bg = background()
    with log_path.open('w') as log:
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=log, text=True)
        try:
            until = time.monotonic() + 90
            while process.poll() is None:
                if (sample := proc_stats(process.pid)):
                    rss.append(sample['rss_kib'])
                if time.monotonic() >= until:
                    raise TimeoutError(f'client timed out: {args}')
                time.sleep(.05)
            stdout = process.stdout.read()
            if process.returncode:
                raise RuntimeError(f'benchmark failed: {args}; see {log_path}')
            result = json.loads(stdout)
        finally:
            stop(process)
            process.stdout.close()
    after = cpu_snapshot()
    total = sum(after['cpu'][i] - before['cpu'][i] for i in range(8))
    result['vm_steal_percent'] = 100 * (after['cpu'][7] - before['cpu'][7]) / max(1, total)
    result.update(bg)
    result['client_sampled_peak_rss_kib'] = max(rss, default=0)
    return result


def vhost(case, enabled, repeat, args, output):
    with tempfile.TemporaryDirectory(prefix='nbs7885-vhost-') as directory:
        socket = str(Path(directory) / 'vhost.sock')
        base = [str(args.server), '--socket-path', socket, '--serial', 'latency-benchmark',
                '--queue-count', '1', '--device-backend', case['backend'], '--no-sync',
                '--log-type', 'console', '--verbose', 'error', '--blockstore-service-pid', str(os.getpid())]
        if case['backend'] == 'null':
            base += ['--device', 'null:1073741824:0']
        else:
            data = Path(directory) / 'device.bin'
            data.write_bytes(b'Z' * 1048576)
            for part in range(case.get('parts', 1)):
                chunk = 1048576 // case.get('parts', 1)
                base += ['--device', f'{data}:{chunk}:{part * chunk}']
        if enabled:
            config = ('EnableLatency: true LatencyThresholdVersion: 1 '
                      'LatencyThresholds { MediaKind: 3 Write: false StartBytes: 0 EndBytes: 1099511627776 ThresholdUs: 1000000 } '
                      'LatencyThresholds { MediaKind: 3 Write: true StartBytes: 0 EndBytes: 1099511627776 ThresholdUs: 1000000 }')
            base += ['--latency-config', config, '--latency-media-kind', '3']
        tag = f"{case['name']}-{'on' if enabled else 'off'}-{repeat}"
        with (output / f'{tag}-server.log').open('w') as log:
            server = subprocess.Popen(['taskset', '-c', SERVER_CPUS, *base], stdout=subprocess.PIPE, stderr=log, text=True)
            samples = []
            try:
                until = time.monotonic() + 20
                while not Path(socket).exists():
                    if server.poll() is not None:
                        raise RuntimeError(f'server startup failed: {tag}; see server log')
                    if time.monotonic() >= until:
                        raise TimeoutError(f'server startup timed out: {tag}')
                    time.sleep(.05)
                client_args = ['taskset', '-c', CLIENT_CPUS, str(args.binary), 'vhost', socket,
                               str(case['bytes']), str(case['depth']), str(args.io_seconds),
                               str(case['write_percent']), str(server.pid)]
                before = cpu_snapshot()
                bg = background()
                with (output / f'{tag}-client.log').open('w') as client_log:
                    client = subprocess.Popen(client_args, stdout=subprocess.PIPE, stderr=client_log, text=True)
                    try:
                        until = time.monotonic() + args.io_seconds + 45
                        while client.poll() is None:
                            if (s := proc_stats(server.pid)):
                                samples.append(s['rss_kib'])
                            if time.monotonic() >= until:
                                raise TimeoutError(f'vhost workload timed out: {tag}')
                            time.sleep(.05)
                        stdout = client.stdout.read()
                        if client.returncode:
                            raise RuntimeError(f'vhost client failed: {tag}; see client log')
                        result = json.loads(stdout)
                    finally:
                        stop(client)
                        client.stdout.close()
                after = cpu_snapshot()
                total = sum(after['cpu'][i] - before['cpu'][i] for i in range(8))
                result['vm_steal_percent'] = 100 * (after['cpu'][7] - before['cpu'][7]) / max(1, total)
                result.update(bg)
                result['server_peak_rss_kib'] = max(samples, default=0)
                result['server_mean_rss_kib'] = statistics.mean(samples) if samples else 0
                server.send_signal(signal.SIGUSR1)
                if not select.select([server.stdout], [], [], 5)[0]:
                    raise TimeoutError(f'stats timed out: {tag}')
                snapshot = json.loads(server.stdout.readline())
                if enabled:
                    latency = snapshot.get('latency')
                    if not latency or latency['threshold_version'] != 1 or not latency['generation']:
                        raise AssertionError(f'latency metadata missing or invalid: {tag}')
                    measured = sum(latency[k]['good'] + latency[k]['bad'] + latency[k]['unknown'] for k in ('read', 'write'))
                    if measured < result['operations']:
                        raise AssertionError(f'missing original operations: {tag}, {measured}, {result}')
                    result['sli_recorded_operations'] = measured
                    result['sli_unknown_operations'] = sum(latency[k]['unknown'] for k in ('read', 'write'))
                    if result['sli_unknown_operations']:
                        raise AssertionError(f'incomplete external timing: {tag}')
                elif 'latency' in snapshot:
                    raise AssertionError(f'diagnostics unexpectedly enabled: {tag}')
                (output / f'{tag}-stats.json').write_text(json.dumps(snapshot, indent=2) + '\n')
                return result
            finally:
                stop(server)
                server.stdout.close()


def summarize(rows, destination):
    metrics = ['ops_per_sec', 'ns_per_op', 'p50_us', 'p99_us', 'cpu_us_per_op',
               'server_cpu_us_per_op', 'max_rss_kib', 'server_mean_rss_kib',
               'server_peak_rss_kib', 'wire_bytes', 'vm_steal_percent']
    records = []
    for name in sorted({r['scenario'] for r in rows}):
        modes = {}
        for enabled in [False, True]:
            group = [r for r in rows if r['scenario'] == name and r['enabled'] == enabled]
            if group:
                modes[enabled] = group
        if len(modes) != 2:
            continue
        for metric in metrics:
            off = [r[metric] for r in modes[False] if metric in r]
            on = [r[metric] for r in modes[True] if metric in r]
            if not off or not on:
                continue
            a, b = statistics.median(off), statistics.median(on)
            paired = []
            for rep in {r['repeat'] for r in modes[False]} & {r['repeat'] for r in modes[True]}:
                x = next(r.get(metric) for r in modes[False] if r['repeat'] == rep)
                y = next(r.get(metric) for r in modes[True] if r['repeat'] == rep)
                if x and y is not None:
                    paired.append((y / x - 1) * 100)
            records.append({'scenario': name, 'metric': metric, 'off_median': a,
                            'on_median': b, 'change_percent': (b / a - 1) * 100 if a else None,
                            'off_min': min(off), 'off_max': max(off), 'on_min': min(on), 'on_max': max(on),
                            'repeats': min(len(off),len(on)),
                            'paired_change_median_percent': statistics.median(paired) if paired else None,
                            'paired_change_min_percent': min(paired) if paired else None,
                            'paired_change_max_percent': max(paired) if paired else None})
    with destination.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]) if records else ['scenario'])
        writer.writeheader()
        writer.writerows(records)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--io-seconds', type=float, default=5)
    parser.add_argument('--component-seconds', type=float, default=3)
    parser.add_argument('--only', default='all', choices=['all','vhost','components'])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    cases = [
        dict(name='null-4k-read-qd1', backend='null', bytes=4096, depth=1, write_percent=0),
        dict(name='null-4k-read-qd32', backend='null', bytes=4096, depth=32, write_percent=0),
        dict(name='null-4k-mixed-qd32', backend='null', bytes=4096, depth=32, write_percent=50),
        dict(name='null-1m-read-qd32', backend='null', bytes=1048576, depth=32, write_percent=0),
        dict(name='aio-4k-read-qd32', backend='aio', bytes=4096, depth=32, write_percent=0, parts=1),
        dict(name='aio-1m-16parts-qd32', backend='aio', bytes=1048576, depth=32, write_percent=0, parts=16),
    ]
    components = [('split-1', 'split', 1), ('split-16', 'split', 16), ('split-128', 'split', 128),
                  ('quota-profile', 'quota', 1), ('quota-pressure', 'quota', 4),
                  ('fifo-unknown-32', 'fifo', 32), ('fifo-unknown-256', 'fifo', 256),
                  ('checkpoint-durable', 'checkpoint', None)]
    metadata = {
        'commit': command(['git','rev-parse','HEAD'], cwd=ROOT).strip(),
        'branch': command(['git','branch','--show-current'], cwd=ROOT).strip(),
        'submodule': command(['git','submodule','status','cloud/contrib/vhost'], cwd=ROOT).strip(),
        'uname': command(['uname','-a']).strip(), 'lscpu': command(['lscpu']),
        'server_cpus': SERVER_CPUS, 'client_cpus': CLIENT_CPUS, 'component_cpu': COMPONENT_CPU,
        'flags': 'release -r; no sanitizers; diagnostics off/on, same binary',
        'comparison': 'Marginal cost of enabling diagnostics on this prototype; does not measure disabled prototype vs main.',
        'harness_sha256': hashlib.sha256((HERE/'main.cpp').read_bytes()).hexdigest(),
        'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'cpu_topology': {cpu: Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list').read_text().strip() for cpu in (16,17,18,19,20)},
        'repeats': args.repeats, 'io_seconds': args.io_seconds, 'component_seconds': args.component_seconds,
        'warmup_seconds': 1, 'start_time_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
        'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        'server_sha256': hashlib.sha256(args.server.read_bytes()).hexdigest(),
        'sampling': 'Components sample 1/64 operation durations; endpoint samples 1/8 observed completions, capped at 1048576 samples. p99 includes client observation delay.',
        'scope': 'External vhost with null or local-file AIO; CPU-only split service with synchronous graph-emitting leaf; simulated-time production quota policy; unknown FIFO invalidation loop; real persistent batch tracker.',
        'checkpoint_off_means': 'Latency batch validation enabled but no persistence; on adds file+directory fsync.',
        'fifo_scope': 'Isolated cost of the actor weak_ptr locking and invalidation loop over graph objects; baseline is loop/harness overhead, not a full volume actor.',
        'initial_background': background(),
    }
    (args.output/'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
    rows = []
    raw = args.output / 'raw.jsonl'
    if raw.exists():
        raise RuntimeError('output already has raw results; choose a fresh output directory')
    jobs = []
    for repeat in range(args.repeats):
        workload = []
        if args.only in ('all','vhost'):
            workload += [('vhost', case) for case in cases]
        if args.only in ('all','components'):
            workload += [('component', case) for case in components]
        random.Random(7885 + repeat).shuffle(workload)
        for family, case in workload:
            modes = [False, True] if repeat % 2 == 0 else [True, False]
            for enabled in modes:
                jobs.append((repeat, family, case, enabled))
    with raw.open('w', buffering=1) as f:
        for index, (repeat, family, case, enabled) in enumerate(jobs):
            name = case['name'] if family == 'vhost' else case[0]
            print(f'{index+1}/{len(jobs)} {name} {"on" if enabled else "off"} repeat={repeat+1}', flush=True)
            if family == 'vhost':
                result = vhost(case, enabled, repeat, args, args.output)
            else:
                _, mode, parameter = case
                tag = f'{name}-{int(enabled)}-{repeat}'
                with tempfile.TemporaryDirectory(prefix='nbs7885-checkpoint-') as directory:
                    arg = str(Path(directory)/'state') if mode == 'checkpoint' else str(parameter)
                    result = run_client(['taskset','-c',COMPONENT_CPU,str(args.binary),mode,str(int(enabled)),arg,
                                         str(args.component_seconds)], args.output/f'{tag}.log')
            result.update(scenario=name, enabled=enabled, repeat=repeat, family=family)
            rows.append(result)
            f.write(json.dumps(result,sort_keys=True)+'\n')
            print(f'  {result["ops_per_sec"]:.0f} ops/s; p99={result["p99_us"]:.3f} us; cpu={result["cpu_us_per_op"]:.3f} us/op', flush=True)
            summarize(rows,args.output/'summary.csv')
    for name in ('quota-profile','quota-pressure'):
        hashes = {r['decision_checksum'] for r in rows if r['scenario']==name}
        if len(hashes)>1:
            raise AssertionError(f'quota decisions changed: {name}: {hashes}')
    metadata['end_time_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    metadata['completed_runs']=len(rows)
    (args.output/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    print('Completed',len(rows),'runs',flush=True)

if __name__=='__main__':
    main()
