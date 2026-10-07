#!/usr/bin/env python3
"""Remaining diagnostic overhead on the optimized split service."""
import argparse, importlib.util, json, random, time, hashlib
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('runner',HERE/'run.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
p.add_argument('--repeats',type=int,default=5);p.add_argument('--seconds',type=float,default=2)
args=p.parse_args();args.output.mkdir(parents=True,exist_ok=True)
if (args.output/'raw.jsonl').exists():raise RuntimeError('choose fresh output directory')
meta=dict(comparison='Optimized implementation: diagnostics disabled vs enabled; split service with synchronous no-I/O leaf.',
 start_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),repeats=args.repeats,seconds=args.seconds,warmup_seconds=1,
 source_commit='f41128ab568c1cd34d7d2d9e3c37384d5214fa47',binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
 harness_sha256=hashlib.sha256((HERE/'main.cpp').read_bytes()).hexdigest(),component_cpu=runner.COMPONENT_CPU,
 completion_scope='completion_p50/p99 measures service invocation to resolved future; p50/p99 also includes preparation and classification.')
(args.output/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
rows=[]
with (args.output/'raw.jsonl').open('w',buffering=1) as f:
 for rep in range(args.repeats):
  parts=[1,16,128];random.Random(7885+rep).shuffle(parts)
  for n in parts:
   for enabled in ([False,True] if rep%2==0 else [True,False]):
    print(f'{len(rows)+1}/{args.repeats*6} split-{n} enabled={enabled} repeat={rep+1}',flush=True)
    r=runner.run_client(['taskset','-c',runner.COMPONENT_CPU,str(args.binary),'split',str(int(enabled)),str(n),str(args.seconds)],args.output/f'split-{n}-{int(enabled)}-{rep}.log')
    r.update(scenario=f'split-{n}',enabled=enabled,repeat=rep);rows.append(r);f.write(json.dumps(r,sort_keys=True)+'\n')
    print(f'{r["ops_per_sec"]:.0f} ops/s; response p99={r["completion_p99_us"]:.3f} us',flush=True)
runner.summarize(rows,args.output/'summary.csv')
# The existing writer uses portable CSV CRLF; keep repository artifacts as LF.
p=args.output/'summary.csv';p.write_bytes(p.read_bytes().replace(b'\r\n',b'\n'))
meta.update(completed_runs=len(rows),end_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
(args.output/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
print('Completed',len(rows),'runs',flush=True)
