#!/usr/bin/env python3
"""Run the exact-format codec matrix, retaining every operation and identity."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

CODECS = ["lz4", "snappy", "zstd-fast", "zstd-1", "zstd-3", "fastlz"]
CHUNKS = [16384, 32768, 65536, 131072, 262144, 4194304]


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def cpu_list(text):
    result = set()
    for item in text.strip().split(","):
        bounds = item.split("-")
        result.update(range(int(bounds[0]), int(bounds[-1]) + 1))
    return result


def busy_ticks(cpu):
    for line in Path("/proc/stat").read_text().splitlines():
        fields = line.split()
        if fields[0] == "cpu" + str(cpu):
            # user/nice/system/irq/softirq/steal; guest is already in user.
            values = list(map(int, fields[1:]))
            return sum(values[i] for i in (0, 1, 2, 5, 6, 7))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    cfg = json.loads(args.manifest.read_text())
    require(cfg["build_type"] == "release" or (args.smoke and cfg["build_type"] == "debug"),
            "release binary required outside explicit debug --smoke")
    require(sha(args.binary) == cfg["binary_sha256"], "binary mismatch")
    require(cfg["source_fingerprint"] and cfg["compiler"] and cfg["build_command"],
            "build provenance required")
    require(cfg["repeats"] >= (1 if args.smoke else 10), "at least ten repeats required")
    cpu_root = Path("/sys/devices/system/cpu") / ("cpu" + str(args.cpu))
    require(args.cpu in os.sched_getaffinity(0), "CPU outside allowed affinity")
    siblings = cpu_list((cpu_root / "topology/thread_siblings_list").read_text()) - {args.cpu}
    llc = 0
    for index in (cpu_root / "cache").glob("index*"):
        if (index / "level").read_text().strip() == "3":
            value = (index / "size").read_text().strip()
            llc = max(llc, int(value[:-1]) * (1024 if value[-1] == "K" else 1024**2))
    cpu_info = Path("/proc/cpuinfo").read_text()
    require(args.smoke or ("hypervisor" not in cpu_info and llc > 0),
            "physical-core/LLC evidence unavailable; only --smoke is permitted")
    categories = {item["category"] for item in cfg["corpora"]}
    require(args.smoke or categories >= {"random", "repeated", "code-vm", "working-merged"},
            "all four corpus categories required")
    for item in cfg["corpora"]:
        path = Path(item["path"])
        require(item["origin"] and item["period"] and item["selection_rule"], "corpus provenance")
        require(sha(path) == item["sha256"] and sha(item["reads"]) == item["reads_sha256"],
                "corpus/trace mismatch")
        require(args.smoke or path.stat().st_size > llc, "working set must exceed LLC")
        if item["category"] == "working-merged":
            require(item["anonymized"] and not path.stat().st_mode & 0o077,
                    "working corpus must be anonymized and owner-only")
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    identity = {"manifest": cfg, "cpu": args.cpu, "siblings": sorted(siblings),
                "llc_bytes": llc, "cpuinfo": cpu_info, "platform": platform.platform(),
                "smoke": args.smoke, "binary_sha256": sha(args.binary)}
    (args.output / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
    os.sched_setaffinity(0, {args.cpu})
    results = []
    for corpus_index, item in enumerate(cfg["corpora"]):
        for codec in CODECS:
            for chunk in CHUNKS:
                name = f"{corpus_index}-{codec}-{chunk}"
                command = [str(args.binary.resolve()), item["path"], item["reads"],
                           codec, str(chunk), str(cfg["repeats"])]
                before = {cpu: busy_ticks(cpu) for cpu in siblings}
                start = time.monotonic()
                with (args.output / (name + ".tsv")).open("w") as out, \
                     (args.output / (name + ".stderr")).open("w") as err:
                    run = subprocess.run(command, stdout=out, stderr=err, timeout=cfg["timeout_seconds"])
                duration = time.monotonic() - start
                sibling_busy = {str(cpu): (busy_ticks(cpu) - before[cpu]) /
                                os.sysconf("SC_CLK_TCK") / duration for cpu in siblings}
                record = {"command": command, "exit_code": run.returncode,
                          "seconds": duration, "sibling_busy_fraction": sibling_busy,
                          "result_sha256": sha(args.output / (name + ".tsv"))}
                results.append(record)
                (args.output / "executions.json").write_text(json.dumps(results, indent=2) + "\n")
                require(run.returncode == 0, "benchmark failed; inspect saved stderr")
                require(args.smoke or all(x < .01 for x in sibling_busy.values()),
                        "sibling was busy; this run cannot satisfy the physical-core gate")
                with (args.output / (name + ".tsv")).open() as stream:
                    rows = list(csv.DictReader(stream, delimiter="\t"))
                require(rows and {int(x["repeat"]) for x in rows} == set(range(cfg["repeats"])),
                        "missing repetitions")
                # Raw per-operation samples are authoritative. Totals exclude
                # repeated runs when computing storage savings for the corpus.
                encoded = [x for x in rows if x["kind"] == "encode" and x["repeat"] == "0"]
                raw = sum(int(x["logical_bytes"]) for x in encoded)
                payload = sum(int(x["payload_bytes"]) for x in encoded)
                metadata = sum(int(x["metadata_bytes"]) for x in encoded)
                record.update(raw_bytes=raw, payload_bytes=payload, metadata_bytes=metadata,
                              ratio=raw / (payload + metadata),
                              savings_percent=100 * (1 - (payload + metadata) / raw),
                              fallback_blobs=sum(x["accepted"] == "0" for x in encoded),
                              fallback_raw_bytes=sum(int(x["logical_bytes"]) for x in encoded
                                                     if x["accepted"] == "0"))
                (args.output / "executions.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps({"variants": len(results), "smoke": args.smoke,
                      "release_gate_passed": False}))


if __name__ == "__main__":
    main()
