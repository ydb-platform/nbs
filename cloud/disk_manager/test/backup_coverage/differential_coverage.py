#!/usr/bin/env python3
"""Differential statement coverage. Ambiguous historical mappings FAIL CLOSED."""
import sys, argparse, collections, difflib, hashlib, json, os, pathlib, re, subprocess, tarfile
PRS=[("7175","872fb1aea9a3badbcd879363658e64e7bf836056","11bf043f6b6055c4ded599421075afb4af4f4b3b"),("7222","8ce7b078a944dbe1e2b3eb55f73a7d7b47dbdfd9","f8fe79c76cae19902d1c092edc636fd92d44c5ee")]
def git(*args):return subprocess.check_output(["git",*args],cwd=REPO).decode()
def names(a,b):
 out={}
 for line in git("diff","--name-status","-M",a,b,"--","*.go").splitlines():
  v=line.split("\t");out[v[-1]]=(v[0],v[1] if v[0].startswith("R") else v[-1])
 return out
def eligible(p):return p.endswith(".go") and not (p.endswith("_test.go") or p.endswith(".pb.go") or any(x in p.split("/") for x in ("mocks","mock","testcommon","tests","test")))
ALIASES={"NewFollowerS3":"NewS3","FollowerS3":"S3","clearCompletedBackupChunkQueueEntries":"clearCompletedBackupChunks","ClearCompletedBackupChunkQueueEntries":"ClearCompletedBackupChunks"}
STORAGE="cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/"
MOVED={STORAGE+"storage_ydb_backup.go":[STORAGE+"storage_ydb.go",STORAGE+"storage_ydb_impl.go",STORAGE+"common.go"]}
def canonical(text):
 for old,new in ALIASES.items():text=text.replace(old,new)
 return text
def key(s):return (canonical(s["Function"]),s["Kind"],canonical(s["Tokens"]))
def matching(old,new):
 # Functions cannot steal identical returns/assignments from unrelated functions.
 pairs={}; groups=set(canonical(s["Function"]) for s in old+new)
 for g in sorted(groups):
  oi=[i for i,s in enumerate(old) if canonical(s["Function"])==g];ni=[i for i,s in enumerate(new) if canonical(s["Function"])==g]
  for b in difflib.SequenceMatcher(None,[key(old[i]) for i in oi],[key(new[i]) for i in ni],autojunk=False).get_matching_blocks():
   for k in range(b.size):pairs[oi[b.a+k]]=ni[b.b+k]
 return pairs
def blocks(data):
 pos=re.search(r"Pos: \[3 \* \d+\]uint32\{(.*?)\n\s*\},",data,re.S)
 num=re.search(r"NumStmt: \[\d+\]uint16\{(.*?)\n\s*\},",data,re.S)
 if not pos or not num:raise ValueError("Unsupported go cover layout")
 pp=re.findall(r"(\d+),\s*(\d+),\s*(0x[0-9a-f]+),",pos.group(1))
 nn=re.findall(r"(\d+),\s*//",num.group(1));assert len(pp)==len(nn)
 return [dict(start=[int(a),int(c,16)&65535],end=[int(b),int(c,16)>>16],statements=int(n)) for (a,b,c),n in zip(pp,nn)]
def reviewed_match(old,new,path,locations,rev):
 rows=json.loads(pathlib.Path(__file__).with_name("reviewed_statement_mapping.json").read_text())
 result={}
 for row in rows:
  source,target=row["source"],row["target"]
  if source["path"]!=path:continue
  if target["revision"]!=rev and git("show",target["revision"]+":"+target["path"])!=git("show",rev+":"+target["path"]):raise ValueError("Reviewed mapping needs re-review for changed product bytes")
  oi=[i for i,s in enumerate(old) if s["Start"]==source["start"] and s["Function"]==source["function"] and s["Kind"]==source["kind"] and hashlib.sha256(s["Tokens"].encode()).hexdigest()==source["tokens_sha256"]]
  ni=[i for i,s in enumerate(new) if locations[i][0]==target["path"] and s["Start"]==target["start"] and s["Function"]==target["function"] and s["Kind"]==target["kind"] and hashlib.sha256(s["Tokens"].encode()).hexdigest()==target["tokens_sha256"]]
  if len(oi)!=1 or len(ni)!=1:raise ValueError("Reviewed mapping no longer matches source: "+str(row))
  result[oi[0]]=ni[0]
 return result

def main():
 global REPO
 ap=argparse.ArgumentParser();ap.add_argument("--repo",required=True);ap.add_argument("--go",required=True);ap.add_argument("--work",required=True);ap.add_argument("--profiles",nargs="*",default=[])
 a=ap.parse_args();REPO=pathlib.Path(a.repo);out=pathlib.Path(a.work);out.mkdir(parents=True,exist_ok=True)
 rev=git("rev-parse","HEAD").strip(); ds=[names(p,h) for _,p,h in PRS]; rename=names(PRS[0][2],PRS[1][2]); headnames=names(PRS[1][2],rev)
 forward={old:p for p,(st,old) in rename.items() if st.startswith("R")}
 forwardhead={old:p for p,(st,old) in headnames.items() if st.startswith("R")}
 allpaths=set(ds[0])|set(ds[1]);excluded=sorted(p for p in allpaths if not eligible(p));allpaths={p for p in allpaths if eligible(p)}
 requests={}; texts={}; missing=[]
 def get(r,p):
  k=(r,p)
  if k in requests:return
  z=subprocess.run(["git","show",r+":"+p],cwd=REPO,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  if z.returncode:return
  text=z.stdout.decode()
  if re.search(r"(?m)^// Code generated .*DO NOT EDIT",text):return
  dest=out/"sources"/r/p;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text(text);requests[k]=str(dest);texts[k]=text
 for pr,(n,pa,he) in enumerate(PRS):
  for p in sorted(allpaths & set(ds[pr])):
   st,old=ds[pr][p]
   if st!="D":get(he,p)
   if st!="A":get(pa,old)
 for p in sorted(allpaths):
  p2=forward.get(p,p);get(PRS[1][2],p2);get(rev,forwardhead.get(p2,p2))
 for candidates in MOVED.values():
  for p in candidates:get(rev,p)
 env=dict(os.environ,GO111MODULE="off")
 tool=pathlib.Path(__file__).with_name("statements.go")
 proc=subprocess.run([a.go,"run","-p=64",str(tool)],input=json.dumps(list(requests.values())).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,cwd=out,timeout=120)
 (out/"parser.stderr").write_bytes(proc.stderr)
 if proc.returncode:raise RuntimeError("Statement parser failed: "+proc.stderr.decode())
 (out/"statements.json").write_bytes(proc.stdout)
 parsed={r["File"]:r["Statements"] for r in json.loads(proc.stdout)}
 ss={k:parsed[v] for k,v in requests.items()}; chosen=collections.defaultdict(set); origins=collections.defaultdict(list); retired=[];unmapped=[]
 for idx,(n,pa,he) in enumerate(PRS):
  for p in sorted(allpaths & set(ds[idx])):
   st,old=ds[idx][p]
   if st=="D":continue
   new=ss.get((he,p),[]);parent=ss.get((pa,old),[])
   unchanged=set(matching(parent,new).values())
   changed=set(range(len(new)))-unchanged
   origins[(he,p)]=[{"index":i,"pr":n,"start":new[i]["Start"],"function":new[i]["Function"]} for i in sorted(changed)]
   chosen[(he,p)]|=changed
 base=PRS[1][2]
 union=collections.defaultdict(set)
 for (r,p),indices in chosen.items():
  if r==base:union[p]|=indices;continue
  p2=forward.get(p,p);new=ss.get((base,p2));old=ss[(r,p)]
  if new is None:retired.append({"path":p,"reason":"deleted before second squash","statements":len(indices)});continue
  m=matching(old,new);union[p2]|={m[i] for i in indices if i in m}
  for i in sorted(indices-set(m)):
   # Changed second-PR statements replace superseded first-PR statements.
   repl=[j for j in chosen.get((base,p2),[]) if canonical(new[j]["Function"])==canonical(old[i]["Function"])]
   if repl:retired.append({"path":p,"start":old[i]["Start"],"reason":"replaced by second PR; replacement counted once"})
   else:unmapped.append({"path":p,"start":old[i]["Start"],"phase":"first-to-second","function":old[i]["Function"]})
 review=json.loads(pathlib.Path(__file__).with_name("reviewed_retired_statements.json").read_text())
 assert (review["source_revision"],review["target_revision"])==(PRS[0][2],PRS[1][2])
 for path,expected in review["source_files"].items():
  if hashlib.sha256(texts[(PRS[0][2],path)].encode()).hexdigest()!=expected:raise ValueError("Retired source no longer matches reviewed bytes: "+path)
 norm=lambda rows:sorted(json.dumps(row,sort_keys=True) for row in rows)
 if norm(retired)!=norm(review["entries"]):raise ValueError("Retired/replaced statements require explicit semantic review")
 final=collections.defaultdict(set)
 for p,indices in union.items():
  if not indices:continue
  hp=forwardhead.get(p,p);old=ss.get((base,p),[])
  candidates=MOVED.get(p,[hp]);merged=[];locations=[]
  for candidate in candidates:
   for j,statement in enumerate(ss.get((rev,candidate),[])):
    merged.append(statement);locations.append((candidate,j))
  if not merged:unmapped.append({"path":p,"phase":"second-to-head","reason":"file removed; needs semantic review","statements":len(indices)});continue
  m=matching(old,merged);m.update(reviewed_match(old,merged,p,locations,rev))
  for i in indices & set(m):
   path,j=locations[m[i]];final[path].add(j)
  for i in sorted(indices-set(m)):unmapped.append({"path":p,"start":old[i]["Start"],"function":old[i]["Function"],"kind":old[i]["Kind"],"tokens_sha256":hashlib.sha256(old[i]["Tokens"].encode()).hexdigest(),"phase":"second-to-head","reason":"modified/deleted statement; not silently dropped"})
 profiles={};inputs=[]
 for name in a.profiles:
  path=pathlib.Path(name);raw=path.read_bytes();inputs.append({"path":str(path),"sha256":hashlib.sha256(raw).hexdigest()})
  with tarfile.open(path) as tar:
   for member in tar.getmembers():
    if not member.isfile():continue
    content=tar.extractfile(member).read().decode()
    for line in content.splitlines()[1:]:
     m=re.fullmatch(r"(.*?):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)",line)
     if not m:raise ValueError("Invalid cover line: "+line)
     path=m[1];path=path[path.index("cloud/"):] if "cloud/" in path else path
     k=(path,*map(int,m.groups()[1:6]));profiles[k]=max(profiles.get(k,0),int(m[7]))
 rows=[];ambiguity=[];included=covered=0
 for p,indices in sorted(final.items()):
  if (REPO/p).read_text()!=texts[(rev,p)]:raise ValueError("Product source differs from mapped revision: "+p)
  stmts=ss[(rev,p)];instrumented=out/"instrumented"/p;instrumented.parent.mkdir(parents=True,exist_ok=True)
  subprocess.run([a.go,"tool","cover","-mode=set","-var=NBS7923Cover","-o",str(instrumented),requests[(rev,p)]],check=True,timeout=30)
  seen=set()
  for b in blocks(instrumented.read_text()):
   ids=[i for i,s in enumerate(stmts) if tuple(b["start"])<=tuple(s["Start"])<tuple(b["end"])]
   feature=set(ids)&indices
   if not feature:continue
   seen|=feature
   if len(ids)!=b["statements"]:
    ambiguity.append({"path":p,**b,"ast_count":len(ids),"feature_positions":[stmts[i]["Start"] for i in sorted(feature)]});continue
   k=(p,*b["start"],*b["end"],b["statements"]);hits=profiles.get(k,0)
   n=len(feature);included+=n;covered+=n if hits else 0
   rows.append({"path":p,**b,"new_statements":n,"old_statements":len(ids)-n,"covered":bool(hits),"profile_present":k in profiles,"positions":[stmts[i]["Start"] for i in sorted(feature)]})
  for i in indices-seen:ambiguity.append({"path":p,"statement":stmts[i],"reason":"no covering block"})
 report={"revision":rev,"deltas":PRS,"reviewed_identifier_renames":ALIASES,"reviewed_file_moves":MOVED,"reviewed_modified_statements":json.loads(pathlib.Path(__file__).with_name("reviewed_statement_mapping.json").read_text()),"profiles":inputs,"excluded":excluded,"retired_or_replaced":retired,"retirement_review":review,"mapping_unresolved":unmapped,"accounting_unresolved":ambiguity,"covered_mapped_statements":covered,"mapped_statements":included,"mapped_percent":100*covered/included if included else 0,"complete":not unmapped and not ambiguity,"passes":not unmapped and not ambiguity and included>0 and 100*covered>=90*included,"blocks":rows,"uncovered":[r for r in rows if not r["covered"]]}
 (out/"report.json").write_text(json.dumps(report,indent=2)+"\n")
 (out/"scope.json").write_text(json.dumps([{"revision":r,"path":p,"changes":v} for (r,p),v in origins.items()],indent=2)+"\n")
 print(json.dumps({k:report[k] for k in ("revision","covered_mapped_statements","mapped_statements","mapped_percent","complete","passes")}));print("UNMAPPED",len(unmapped),"ACCOUNTING",len(ambiguity),"REPORT",str(out/"report.json"))
 return 0 if report["passes"] else 1
if __name__=="__main__":sys.exit(main())
