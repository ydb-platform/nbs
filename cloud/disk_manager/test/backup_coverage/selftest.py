#!/usr/bin/env python3
"""Check AST accounting against Go cover, including package-level closures."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--go", required=True)
args = parser.parse_args()
here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("coverage_tool", here / "differential_coverage.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
source = """package sample
var factory = map[string]func() int{"x": func() int { return 1 }}
func example(x int) int {
 old := 1
 added := 2
 if x > 0 { added++ } else if x < 0 { added-- } else { added = 3 }
 f := func() int { return old + added }
 switch x { case 1: old++; default: old-- }
 for i := 0; i < 2; i++ { old += i }
 return f() + old
}
"""
with tempfile.TemporaryDirectory(prefix="backup-cover-selftest-") as work:
    root = Path(work)
    path = root / "sample.go"
    path.write_text(source)
    env = dict(os.environ, GO111MODULE="off")
    parsed = subprocess.check_output(
        [args.go, "run", "-p=64", str(here / "statements.go")],
        input=json.dumps([str(path)]).encode(), cwd=root, env=env)
    statements = json.loads(parsed)[0]["Statements"]
    assert any(s["Function"] == "<package>" for s in statements)
    target = root / "covered.go"
    subprocess.run([args.go, "tool", "cover", "-mode=set", "-var=Counter",
                    "-o", str(target), str(path)], check=True)
    blocks = module.blocks(target.read_text())
    seen = set()
    for block in blocks:
        ids = {i for i, statement in enumerate(statements)
               if tuple(block["start"]) <= tuple(statement["Start"]) < tuple(block["end"])}
        assert len(ids) == block["statements"], (block, ids)
        assert not seen.intersection(ids)
        seen.update(ids)
    assert seen == set(range(len(statements)))
    mixed = next(b for b in blocks if b["start"][0] <= 4 and b["end"][0] >= 5)
    assert mixed["statements"] >= 2  # old and changed statements share a counter.
    print(json.dumps({"passed": True, "statements": len(statements), "blocks": len(blocks),
                      "package_closures": True, "mixed_block": mixed}))
