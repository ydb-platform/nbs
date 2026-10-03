#!/usr/bin/env python3
"""Run checksum resource profiles on newly created BlobStorage-backed NBS disks.

This driver does not change service configuration or binaries. Run separately
for each of the four pinned server/configuration combinations in README.md.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time
import uuid


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sample(pid):
    directory = Path("/proc") / str(pid)
    fields = (directory / "stat").read_text().rsplit(") ", 1)[1].split()
    status = dict(line.split(":", 1) for line in
                  (directory / "status").read_text().splitlines() if ":" in line)
    return {
        "pid": pid,
        "start_ticks": int(fields[19]),
        "cpu_seconds": (int(fields[11]) + int(fields[12])) /
                       os.sysconf("SC_CLK_TCK"),
        "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
        "peak_rss_bytes": int(status["VmHWM"].split()[0]) * 1024,
    }


def load_spec(name, disk, blocks, request_blocks, random, write_rate,
              duration, count, iops, bandwidth):
    return f"""
Vertices {{ Test {{
  Name: "{name}"
  VolumeName: "{disk}"
  MountVolumeRequest {{
    VolumeAccessMode: VOLUME_ACCESS_READ_WRITE
    VolumeMountMode: VOLUME_MOUNT_LOCAL
  }}
  TestDuration: {duration}
  ClientPerformanceProfile {{
    SSDProfile {{
      MaxReadIops: {iops}
      MaxWriteIops: {iops}
      MaxReadBandwidth: {bandwidth}
      MaxWriteBandwidth: {bandwidth}
    }}
  }}
  ArtificialLoadSpec {{ Ranges {{
    Start: 0
    End: {blocks - 1}
    LoadType: {"LOAD_TYPE_RANDOM" if random else "LOAD_TYPE_SEQUENTIAL"}
    IoDepth: {32 if random else 16}
    ReadRate: {100 - write_rate}
    WriteRate: {write_rate}
    MinRequestSize: {request_blocks}
    MaxRequestSize: {request_blocks}
    RequestsCount: {count}
  }} }}
}} }}
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loadtest", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--server-pid", type=int, action="append", default=[])
    parser.add_argument("--storage-config", type=Path)
    parser.add_argument("--label", choices=["off", "legacy", "original", "optimized"],
                        required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bytes", type=int, default=2 * 1024**3)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--iops", type=int, default=32000)
    parser.add_argument("--bandwidth", type=int, default=450 * 1024**2)
    parser.add_argument("--drain-seconds", type=int, default=10)
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if (args.bytes < 2 * 1024**3 or args.bytes % (1024**2) or args.seconds <= 0
            or args.repeats < 1 or args.iops <= 0 or args.bandwidth <= 0
            or args.drain_seconds < 0):
        parser.error("use a whole number of MiB >= 2 GiB and positive workload limits")
    if not args.dry_run and (
            not args.loadtest or not args.server_pid or not args.storage_config):
        parser.error("execution needs --loadtest, --server-pid and --storage-config")
    args.output.mkdir(parents=True, exist_ok=False)
    blocks = args.bytes // 4096
    identity = {
        "label": args.label, "host": args.host, "port": args.port,
        "logical_bytes_per_disk": args.bytes, "block_bytes": 4096,
        "replicas_in_logical_bytes": False, "system": platform.uname()._asdict(),
        "server_pids": args.server_pid, "repeats": args.repeats,
        "driver_sha256": digest(__file__), "created_disks": [],
        "seed": "loadtest native generator; no deterministic seed API",
    }
    if args.loadtest:
        identity["loadtest_sha256"] = digest(args.loadtest)
    if args.storage_config:
        identity["storage_config_sha256"] = digest(args.storage_config)
    identities = {}
    if not args.dry_run:
        for pid in args.server_pid:
            identities[pid] = sample(pid)["start_ticks"]
        identity["server_binaries"] = {
            str(pid): digest(Path("/proc") / str(pid) / "exe")
            for pid in args.server_pid}
        identity["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()

    def capture(phase, stream):
        samples = [sample(pid) for pid in args.server_pid]
        for value in samples:
            if value["start_ticks"] != identities[value["pid"]]:
                raise RuntimeError("server process changed during measurement")
        stream.write(json.dumps({"monotonic": time.monotonic(),
                                 "phase": phase, "processes": samples}) + "\n")
        stream.flush()

    def run(name, config):
        directory = args.output / name
        directory.mkdir()
        config_path = directory / "loadtest.txt"
        config_path.write_text(config)
        if args.dry_run:
            return
        argv = [str(args.loadtest.resolve()), "--host", args.host,
                "--port", str(args.port), "--config", str(config_path.resolve()),
                "--results", str((directory / "results.json").resolve()),
                "--timeout", str(max(1200, args.seconds * 4))]
        (directory / "argv.json").write_text(json.dumps(argv, indent=2))
        with (directory / "resources.jsonl").open("w") as resources:
            capture("before", resources)
            with (directory / "stdout").open("wb") as out, (
                    directory / "stderr").open("wb") as err:
                child = subprocess.Popen(argv, stdout=out, stderr=err)
                (directory / "pid").write_text(str(child.pid))
                try:
                    while child.poll() is None:
                        capture("load", resources)
                        time.sleep(1)
                finally:
                    if child.poll() is None:
                        child.terminate()
                        try:
                            child.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            child.kill()
                            child.wait()
                (directory / "exitcode").write_text(str(child.returncode))
                capture("after", resources)
                if child.returncode:
                    raise RuntimeError(f"{name} failed; inspect {directory}")
            for _ in range(args.drain_seconds):
                time.sleep(1)
                capture("drain", resources)

    for repeat in range(1, args.repeats + 1):
        disk = "nbs-6711-" + uuid.uuid4().hex
        identity["created_disks"].append(disk)
        (args.output / "identity.json").write_text(json.dumps(identity, indent=2))
        run(f"{repeat}-create", f"""
Vertices {{ ControlPlaneAction {{
  Name: "create"
  CreateVolumeRequest {{
    DiskId: "{disk}"
    BlockSize: 4096
    BlocksCount: {blocks}
    StorageMediaKind: STORAGE_MEDIA_SSD
  }}
}} }}
""")
        profiles = [
            ("first-fill", 256, False, 100, 0, blocks // 256),
            ("sequential-read", 256, False, 0, args.seconds, 0),
            ("overwrite", 256, False, 100, 0, blocks // 256),
            ("repeated-read", 256, False, 0, args.seconds, 0),
            ("random-write", 1, True, 100, args.seconds, 0),
            ("random-read", 1, True, 0, args.seconds, 0),
            ("mixed", 1, True, 30, args.seconds, 0),
        ]
        for name, size, random, write, duration, count in profiles:
            run(f"{repeat}-{name}", load_spec(
                name, disk, blocks, size, random, write, duration, count,
                args.iops, args.bandwidth))
        if args.cleanup:
            run(f"{repeat}-destroy", f"""
Vertices {{ ControlPlaneAction {{
  Name: "destroy"
  DestroyVolumeRequest {{ DiskId: "{disk}" }}
}} }}
""")
    print(json.dumps({"output": str(args.output),
                      "dry_run": args.dry_run,
                      "created_disks": identity["created_disks"]}))


if __name__ == "__main__":
    main()
