#!/usr/bin/env python3
"""Summarize immutable micro/full-path evidence; never authorizes rollout."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import subprocess


def require(value, message):
    if not value:
        raise RuntimeError(message)


def quantiles(values):
    values = sorted(values)
    if not values:
        return {}
    return {
        "count": len(values), "mean": statistics.mean(values),
        "min": values[0], "max": values[-1],
        "stdev": statistics.stdev(values) if len(values) > 1 else 0,
        **{f"p{p}": values[max(0, math.ceil(len(values) * p / 100) - 1)]
           for p in (50, 95, 99)},
    }


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def micro(directory):
    identity = json.loads((directory / "identity.json").read_text())
    executions = json.loads((directory / "executions.json").read_text())
    results = []
    for execution in executions:
        _, corpus, _, codec, chunk, repeats = execution["command"]
        candidates = [p for p in directory.glob(f"*-{codec}-{chunk}.tsv")
                      if sha(p) == execution["result_sha256"]]
        require(len(candidates) == 1, "missing or ambiguous immutable micro result")
        with candidates[0].open() as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        per_repeat = []
        for repeat in range(int(repeats)):
            for kind in ("encode", "read"):
                selected = [r for r in rows if r["kind"] == kind and int(r["repeat"]) == repeat]
                require(selected, "missing micro samples")
                logical = sum(int(r["logical_bytes"]) for r in selected)
                cpu = sum(int(r["cpu_ns"]) for r in selected)
                per_repeat.append({
                    "repeat": repeat, "kind": kind, "logical_bytes": logical,
                    "cpu_seconds_per_gib": cpu / 1e9 / (logical / 1024**3),
                    "cpu_ns_per_operation": cpu / len(selected),
                    "wall_ns": quantiles([int(r["wall_ns"]) for r in selected]),
                    "physical_bytes": sum(int(r["payload_bytes"]) for r in selected),
                    "decoded_chunks": sum(int(r["chunks"]) for r in selected),
                })
        results.append({**execution, "corpus": corpus, "codec": codec,
                        "chunk_bytes": int(chunk), "per_repeat": per_repeat})
    return {"kind": "micro", "directory": str(directory), "identity": identity, "variants": results}


def counter(document, selector):
    if "path" in selector:
        value = document
        for key in selector["path"]:
            value = value[key]
        require(isinstance(value, (int, float)), "counter path is not numeric")
        return value
    labels = selector["labels"]
    found = [sensor["value"] for sensor in document["sensors"]
             if all(sensor.get("labels", {}).get(k) == v for k, v in labels.items())]
    require(len(found) == 1, "counter selector must match exactly one sensor: " + str(labels))
    return found[0]


def full(directory, inventory_binary):
    cfg = json.loads((directory / "manifest.json").read_text())
    summary = json.loads((directory / "summary.json").read_text())
    require(not summary["errors"], "operation errors invalidate comparison")
    require(json.loads((directory / "sampling-errors.json").read_text()) == [],
            "sampling errors invalidate comparison")
    ops = [json.loads(line) for line in (directory / "operations.jsonl").read_text().splitlines()]
    samples = sorted([json.loads(line) for line in
                      (directory / "samples.jsonl").read_text().splitlines()],
                     key=lambda value: value["monotonic_ns"])
    require(len(samples) >= 2 and ops, "empty full-path evidence")
    inventory = subprocess.run(
        [str(inventory_binary), "--inventory", str(directory / "after-describe.json"),
         str(cfg["block_size"])], capture_output=True, text=True, timeout=60, check=True)
    inventory = json.loads(inventory.stdout)
    raw = inventory["logical_bytes"]
    stored = inventory["payload_bytes"] + inventory["metadata_bytes"]
    require(raw and stored, "empty live inventory")
    counters = {}
    for name, selector in cfg["counter_selectors"].items():
        first = counter(samples[0]["metrics"], selector)
        last = counter(samples[-1]["metrics"], selector)
        require(last >= first, "counter decreased: " + name)
        counters[name] = last - first
    latency = {}
    for key in sorted({(op["kind"], op["size"]) for op in ops}):
        chosen = [op for op in ops if (op["kind"], op["size"]) == key]
        latency[f"{key[0]}-{key[1]}"] = {
            "service_ns": quantiles([op["latency_ns"] for op in chosen]),
            "arrival_to_completion_ns": quantiles([op["end_ns"] - op["planned_ns"] for op in chosen]),
            "queue_ns": quantiles([op["queue_ns"] for op in chosen]),
        }
    compressed = counters.get("compressed_foreground_logical_read_bytes", 0)
    raw_reads = counters.get("raw_merged_foreground_logical_read_bytes", 0)
    return {
        "kind": "full", "directory": str(directory), "manifest": cfg, "summary": summary,
        "effective_config": json.loads((directory / "before-effective-config.json").read_text()),
        "inventory": inventory, "latency_by_operation": latency, "counter_deltas": counters,
        "storage_ratio": raw / stored, "savings_percent": 100 * (1 - stored / raw),
        "live_compressed_logical_fraction": inventory["compressed_logical_bytes"] / raw,
        "merged_read_compressed_fraction": compressed / (compressed + raw_reads)
        if compressed + raw_reads else None,
        "evidence_sha256": {p.name: sha(p) for p in directory.iterdir() if p.is_file()},
    }


def compare(runs):
    pairs = []
    baseline = [x for x in runs if x["manifest"]["mode"] == "baseline"]
    invariants = ["source_fingerprint", "binary_sha256", "corpus_sha256",
                  "working_set_bytes", "block_size", "hardware_identity", "cache_policy",
                  "queue_depth", "target_iops", "duration_seconds", "phase", "reads", "series_id"]
    allowed_config = {"directmergedblobcompressionpercentage",
                      "compactionmergedblobcompressionpercentage"}
    for run in runs:
        cfg = run["manifest"]
        if cfg["mode"] == "baseline":
            continue
        candidates = [x for x in baseline if all(x["manifest"].get(k) == cfg.get(k)
                                                 for k in invariants)
                      and x["manifest"]["repetition"] == cfg["repetition"]]
        require(len(candidates) == 1, "each run requires one matching same-code baseline")
        base = candidates[0]
        cleaned = lambda x: {k: v for k, v in x["effective_config"].items()
                             if k.casefold() not in allowed_config}
        require(cleaned(base) == cleaned(run), "non-compression effective config differs")
        changes = {}
        for metric in ("cpu_seconds_per_operation", "cpu_seconds_per_gib", "mean_busy_cores",
                       "cpu_seconds_above_idle", "foreground_iops", "peak_rss_bytes"):
            before, after = base["summary"][metric], run["summary"][metric]
            changes[metric] = {"baseline": before, "measured": after,
                               "delta": after - before,
                               "delta_percent": 100 * (after / before - 1) if before else None}
        for key, value in run["latency_by_operation"].items():
            require(key in base["latency_by_operation"], "operation mix differs")
            for p in ("p50", "p95", "p99"):
                before = base["latency_by_operation"][key]["service_ns"][p]
                after = value["service_ns"][p]
                changes[f"{key}-{p}_ns"] = {
                    "baseline": before, "measured": after,
                    "delta_percent": 100 * (after / before - 1),
                }
        pairs.append({"baseline": base["directory"], "measured": run["directory"],
                      "mode": cfg["mode"], "changes": changes})
    return pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--micro", type=Path, nargs="*", default=[])
    parser.add_argument("--full", type=Path, nargs="*", default=[])
    parser.add_argument("--inventory-binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(args.micro or args.full, "input evidence required")
    require(not args.full or args.inventory_binary, "native inventory binary required")
    report = {"micro": [micro(p) for p in args.micro],
              "full": [full(p, args.inventory_binary.resolve()) for p in args.full],
              "release_gate_passed": False,
              "limitations": [
                  "Live DescribeBlocks inventory excludes unreferenced checkpoint/garbage blobs.",
                  "A storage claim requires an isolated disk without checkpoints and post-GC proof.",
                  "NIC counters include all namespace traffic; select process/route boundaries separately.",
                  "Ten physical-core release repeats and all production CPU classes remain required.",
                  "CPU/latency/memory rollout budgets require a reviewed decision based on these samples.",
              ]}
    report["comparisons"] = compare(report["full"])
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    lines = ["# Merged blob compression measurements", "",
             "Release gates are not implied by this report.", "",
             "| Run | Mode | Ratio | Savings % | Compressed live fraction |",
             "| --- | --- | ---: | ---: | ---: |"]
    for run in report["full"]:
        lines.append(f'| {run["directory"]} | {run["manifest"]["mode"]} | '
                     f'{run["storage_ratio"]:.4f} | {run["savings_percent"]:.3f} | '
                     f'{run["live_compressed_logical_fraction"]:.4f} |')
    lines.extend(["", "Raw and per-repeat CPU/latency/queue/counter values are in report.json.", ""])
    lines.extend("* " + value for value in report["limitations"])
    (args.output / "report.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
