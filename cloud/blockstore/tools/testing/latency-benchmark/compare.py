#!/usr/bin/env python3
"""Paired comparison of original and optimized enabled diagnostics."""
import argparse, csv, hashlib, importlib.util, json, random, statistics, subprocess, tempfile, time
from pathlib import Path
from types import SimpleNamespace
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('runner', HERE/'run.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
CASES = [
 dict(name='null-4k-read-qd1',backend='null',bytes=4096,depth=1,write_percent=0),
 dict(name='null-4k-read-qd32',backend='null',bytes=4096,depth=32,write_percent=0),
 dict(name='null-4k-mixed-qd32',backend='null',bytes=4096,depth=32,write_percent=50),
 dict(name='null-1m-read-qd32',backend='null',bytes=1048576,depth=32,write_percent=0),
 dict(name='aio-4k-read-qd32',backend='aio',bytes=4096,depth=32,write_percent=0,parts=1),
 dict(name='aio-1m-16parts-qd32',backend='aio',bytes=1048576,depth=32,write_percent=0,parts=16),
]
COMPONENTS = [('graph-quota-16','graph',16),('graph-quota-128','graph',128),('split-1','split',1),('split-16','split',16),('split-128','split',128),
 ('quota-profile','quota',1),('quota-pressure','quota',4),
 ('fifo-unknown-32','fifo',32),('fifo-unknown-256','fifo',256),('checkpoint-durable','checkpoint',None)]
def summary(rows,path):
 out=[]
 metrics=['ops_per_sec','ns_per_op','p50_us','p99_us','completion_p50_us','completion_p99_us',
          'cpu_us_per_op','server_cpu_us_per_op','max_rss_kib','server_mean_rss_kib','wire_bytes']
 for name in sorted({r['scenario'] for r in rows}):
  groups={m:[r for r in rows if r['scenario']==name and r['implementation']==m] for m in ('baseline','optimized')}
  if not all(groups.values()):continue
  for metric in metrics:
   if metric.startswith('completion_') and not name.startswith('split-'):continue
   a=[r[metric] for r in groups['baseline'] if metric in r]
   b=[r[metric] for r in groups['optimized'] if metric in r]
   if not a or not b:continue
   pairs=[]
   for rep in {r['repeat'] for r in groups['baseline']} & {r['repeat'] for r in groups['optimized']}:
    x=next(r.get(metric) for r in groups['baseline'] if r['repeat']==rep)
    y=next(r.get(metric) for r in groups['optimized'] if r['repeat']==rep)
    if x and y is not None:pairs.append((y/x-1)*100)
   out.append(dict(scenario=name,metric=metric,baseline_median=statistics.median(a),optimized_median=statistics.median(b),
    baseline_min=min(a),baseline_max=max(a),optimized_min=min(b),optimized_max=max(b),repeats=min(len(a),len(b)),
    paired_change_median_percent=statistics.median(pairs) if pairs else None,
    paired_change_min_percent=min(pairs) if pairs else None,paired_change_max_percent=max(pairs) if pairs else None))
 with path.open('w',newline='') as f:
  writer=csv.DictWriter(f,fieldnames=list(out[0]) if out else ['scenario'],lineterminator='\n')
  writer.writeheader();writer.writerows(out)
def main():
 p=argparse.ArgumentParser()
 for mode in ('baseline','optimized'):
  p.add_argument('--'+mode+'-binary',type=Path,required=True)
  p.add_argument('--'+mode+'-server',type=Path,required=True)
 p.add_argument('--output',type=Path,required=True)
 p.add_argument('--repeats',type=int,default=5)
 p.add_argument('--io-seconds',type=float,default=5)
 p.add_argument('--component-seconds',type=float,default=3)
 p.add_argument('--only',choices=['all','components','vhost'],default='all')
 args=p.parse_args();args.output.mkdir(parents=True,exist_ok=True)
 raw=args.output/'raw.jsonl'
 if raw.exists():raise RuntimeError('choose a fresh output directory')
 modes={m:SimpleNamespace(binary=getattr(args,m+'_binary'),server=getattr(args,m+'_server'),io_seconds=args.io_seconds) for m in ('baseline','optimized')}
 for m in modes:(args.output/m).mkdir()
 meta=dict(comparison='Frozen optimized graph v1 vs compact summary v2, diagnostics enabled in both; checkpoint durable in both.',
  prototype_commit='7ece625356c54c8249212ab9cb16411a448a80b2',baseline_benchmark_commit='f41128ab568c1cd34d7d2d9e3c37384d5214fa47',
  checkout_head=runner.command(['git','rev-parse','HEAD'],cwd=runner.ROOT).strip(),
  start_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),repeats=args.repeats,
  io_seconds=args.io_seconds,component_seconds=args.component_seconds,warmup_seconds=1,
  binary_hashes={m:{k:hashlib.sha256(getattr(v,k).read_bytes()).hexdigest() for k in ('binary','server')} for m,v in modes.items()},
  harness_sha256=hashlib.sha256((HERE/'main.cpp').read_bytes()).hexdigest(),
  baseline_harness_sha256=hashlib.sha256(runner.command(['git','show','f41128ab568c1cd34d7d2d9e3c37384d5214fa47:cloud/blockstore/tools/testing/latency-benchmark/main.cpp'],cwd=runner.ROOT).encode()).hexdigest(),
  harness_changes='Only the summary reader API and removal of the graph node-count accessor; workload and timers unchanged.',
  compare_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
  uname=runner.command(['uname','-a']).strip(),lscpu=runner.command(['lscpu']),initial_background=runner.background(),
  server_cpus=runner.SERVER_CPUS,client_cpus=runner.CLIENT_CPUS,component_cpu=runner.COMPONENT_CPU,
  completion_scope='Split response timer spans service invocation to resolved future, sampled 1/64; loop p50/p99 also includes request preparation and classification. Vhost p50/p99 spans submit to client-observed completion, sampled 1/8.',
  scope='Release binaries; identical workloads and timers, with the summary API adapter recorded separately. Synthetic quota-graph composition uses virtual timestamps without real waits. Split uses real service with synchronous no-I/O leaf. Quota uses simulated time; FIFO isolated weak_ptr/invalidation loop; durable checkpoint uses real fsync. Endpoint uses null or hot local-file AIO, not a production volume actor.')
 (args.output/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
 jobs=[]
 for rep in range(args.repeats):
  work=[]
  if args.only in ('all','vhost'):work += [('vhost',c) for c in CASES]
  if args.only in ('all','components'):work += [('component',c) for c in COMPONENTS]
  random.Random(7885+rep).shuffle(work)
  for family,case in work:
   for mode in (('baseline','optimized') if rep%2==0 else ('optimized','baseline')):jobs.append((rep,family,case,mode))
 rows=[]
 with raw.open('w',buffering=1) as f:
  for i,(rep,family,case,mode) in enumerate(jobs):
   name=case['name'] if family=='vhost' else case[0]
   print(f'{i+1}/{len(jobs)} {name} {mode} repeat={rep+1}',flush=True)
   out=args.output/mode
   if family=='vhost':result=runner.vhost(case,True,rep,modes[mode],out)
   else:
    _,kind,parameter=case
    with tempfile.TemporaryDirectory(prefix='nbs7885-compare-') as directory:
     arg=str(Path(directory)/'state') if kind=='checkpoint' else str(parameter)
     result=runner.run_client(['taskset','-c',runner.COMPONENT_CPU,str(modes[mode].binary),kind,'1',arg,str(args.component_seconds)],out/f'{name}-{rep}.log')
   result.update(scenario=name,implementation=mode,repeat=rep,family=family,latency_enabled=True)
   rows.append(result);f.write(json.dumps(result,sort_keys=True)+'\n');summary(rows,args.output/'summary.csv')
   print(f'  {result["ops_per_sec"]:.0f} ops/s; p99={result["p99_us"]:.3f} us; response p99={result.get("completion_p99_us",0):.3f} us',flush=True)
 for name in ('quota-profile','quota-pressure'):
  if len({r['decision_checksum'] for r in rows if r['scenario']==name})>1:raise AssertionError('quota decisions changed: '+name)
 meta.update(completed_runs=len(rows),end_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
 (args.output/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
 print('Completed',len(rows),'runs',flush=True)
if __name__=='__main__':main()
