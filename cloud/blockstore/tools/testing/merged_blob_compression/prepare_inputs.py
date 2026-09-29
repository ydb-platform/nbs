#!/usr/bin/env python3
"""Create reproducible synthetic smoke corpora and aligned/boundary read traces."""
import argparse
import hashlib
import json
from pathlib import Path
import random


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--reads-per-profile", type=int, default=1024)
    parser.add_argument("--seed", type=int, default=7823)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--build-type", choices=("release", "debug"), default="release")
    parser.add_argument("--source-fingerprint", required=True)
    parser.add_argument("--compiler", required=True)
    parser.add_argument("--build-command", required=True)
    args = parser.parse_args()
    blob = 4 * 1024 * 1024
    if args.bytes < 2 * blob or args.bytes % blob or args.reads_per_profile <= 0:
        parser.error("bytes must be >=8 MiB and a multiple of 4 MiB; reads must be positive")
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    rng = random.Random(args.seed)
    corpora = []
    for category in ("random", "repeated"):
        path = args.output / (category + ".bin")
        with path.open("wb") as stream:
            for index in range(args.bytes // blob):
                stream.write(rng.getrandbits(blob * 8).to_bytes(blob, "little") if category == "random"
                             else bytes([65 + index % 26]) * blob)
        corpora.append({
            "category": category, "path": str(path.resolve()), "sha256": sha(path),
            "origin": "synthetic prepare_inputs.py", "period": "synthetic; no production sampling",
            "selection_rule": f"seed={args.seed}; whole aligned blobs; category={category}",
        })
    profiles = {}
    blocks = args.bytes // 4096
    for size in (4096, blob):
        count = size // 4096
        for order in ("sequential", "random"):
            offsets = []
            for index in range(args.reads_per_profile):
                block = ((index % (args.bytes // size)) * count if order == "sequential"
                         else rng.randrange(blocks - count + 1))
                offsets.append({"offset": block * 4096, "size": size})
            profiles[f"{order}-{size}"] = offsets
    profiles["cross-chunk-4m"] = [
        {"offset": (32768 - 4096 + index * blob) % (args.bytes - blob) // 4096 * 4096,
         "size": blob} for index in range(args.reads_per_profile)
    ]
    combined = [item for values in profiles.values() for item in values]
    trace = args.output / "reads.tsv"
    trace.write_text("".join(f'{item["offset"]} {item["size"]}\n' for item in combined))
    (args.output / "read-profiles.json").write_text(json.dumps(profiles, indent=2) + "\n")
    for corpus in corpora:
        corpus.update(reads=str(trace.resolve()), reads_sha256=sha(trace))
    manifest = {
        "build_type": args.build_type, "binary_sha256": sha(args.binary),
        "source_fingerprint": args.source_fingerprint, "compiler": args.compiler,
        "build_command": args.build_command, "repeats": 1, "timeout_seconds": 600,
        "corpora": corpora,
    }
    (args.output / "micro-smoke.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Synthetic smoke inputs created; production corpus and physical-core gates are not satisfied.")


if __name__ == "__main__":
    main()
