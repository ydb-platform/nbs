"""Run one isolated NBS benchmark phase; retain raw per-operation evidence.

The same executable/config/corpus identity is compared across baseline/enabled
runs. This tool never changes server configuration, creates or destroys a disk.
The explicit disk acknowledgement prevents an accidental write to another disk.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
import mmap
import os
from pathlib import Path
import threading
import time
import urllib.request

from cloud.blockstore.public.sdk.python.client import CreateClient

BLOCK = 4096
MIB = 1024 * 1024


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4 * MIB), b""):
            h.update(chunk)
    return h.hexdigest()


def proc_sample(pids):
    result = {}
    for pid in pids:
        root = Path("/proc") / str(pid)
        fields = (root / "stat").read_text().rsplit(")", 1)[1].split()
        status = {}
        for line in (root / "status").read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:")):
                name, value, _ = line.split()
                status[name[:-1]] = int(value) * 1024
        result[str(pid)] = {
            "start_ticks": int(fields[19]),
            "cpu_ticks": int(fields[11]) + int(fields[12]),
            **status,
        }
    return result


def metrics(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def field(value, name):
    matches = [v for k, v in value.items() if k.casefold() == name.casefold()]
    require(len(matches) == 1, "missing/ambiguous field " + name)
    return matches[0]


def process_identity(pids):
    result = {}
    for pid in pids:
        root = Path("/proc") / str(pid)
        result[str(pid)] = {
            "exe_sha256": digest(root / "exe"),
            "network_namespace": os.readlink(root / "ns/net"),
            "cpu_affinity": sorted(os.sched_getaffinity(pid)),
        }
    return result


def network_sample(pids):
    # Read each namespace once; NIC bytes are not silently assumed to belong
    # to a process, tenant, route or individual disk.
    result = {}
    for pid in pids:
        root = Path("/proc") / str(pid)
        namespace = os.readlink(root / "ns/net")
        if namespace not in result:
            result[namespace] = (root / "net/dev").read_text()
    return result


def cpu_delta(first, last):
    require(first.keys() == last.keys(), "CPU boundary changed")
    ticks = 0
    for pid in first:
        require(first[pid]["start_ticks"] == last[pid]["start_ticks"],
                "process restarted during measurement")
        delta = last[pid]["cpu_ticks"] - first[pid]["cpu_ticks"]
        require(delta >= 0, "process CPU counter decreased")
        ticks += delta
    return ticks / os.sysconf("SC_CLK_TCK")


def quantile(values, p):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * p) - 1)] if values else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--allow-write-disk-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cfg = json.loads(args.manifest.read_text())
    require(args.allow_write_disk_id == cfg["disk_id"], "disk acknowledgement mismatch")
    require(cfg["mode"] in ("baseline", "enabled", "mixed"), "mode")
    require(cfg["phase"] in ("write-4k", "write-4m", "read", "concurrent"), "phase")
    require(cfg["queue_depth"] > 0 and cfg["queue_depth"] <= 128, "queue depth")
    require(cfg["duration_seconds"] > 0 and cfg["target_iops"] > 0, "duration/rate")
    require(cfg["block_size"] == BLOCK, "benchmark profiles require 4096-byte blocks")
    require(cfg["pids"], "explicit NBS/YDB process CPU boundary is required")
    require(cfg["binary_sha256"] == digest(cfg["server_binary"]), "server binary digest")
    require(cfg["corpus_sha256"] == digest(cfg["corpus"]), "corpus digest")
    require(cfg["cache_policy"] and cfg["hardware_identity"] and cfg["config_sha256"],
            "cache/hardware/config evidence required")
    require(cfg["source_fingerprint"] and cfg["series_id"] and cfg["repetition"] >= 0,
            "source/series/repetition identity required")
    require(cfg["idle_seconds"] >= 1, "idle CPU sample required")
    require(cfg["expected_live_config"], "expected effective config required")
    identity = process_identity(cfg["pids"])
    require(cfg["binary_sha256"] in {v["exe_sha256"] for v in identity.values()},
            "declared NBS binary is not running in the measured process boundary")
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    (args.output / "manifest.json").write_text(json.dumps(cfg, indent=2) + "\n")
    (args.output / "boot_id.txt").write_text(
        Path("/proc/sys/kernel/random/boot_id").read_text())
    (args.output / "process-identity.json").write_text(json.dumps(identity, indent=2) + "\n")
    stop = threading.Event()
    samples = []
    sample_errors = []
    def sample():
        try:
            while not stop.is_set():
                samples.append({
                    "monotonic_ns": time.monotonic_ns(),
                    "proc": proc_sample(cfg["pids"]),
                    "metrics": metrics(cfg["metrics_url"]),
                    "net_dev": Path("/proc/net/dev").read_text(),
                    "network_namespaces": network_sample(cfg["pids"]),
                })
                stop.wait(cfg.get("sample_period_seconds", 1))
        except Exception as e:
            sample_errors.append(str(e))
    sampler = threading.Thread(target=sample)
    operations = []
    corpus_file = open(cfg["corpus"], "rb")
    corpus = mmap.mmap(corpus_file.fileno(), 0, access=mmap.ACCESS_READ)
    require(len(corpus) % BLOCK == 0 and len(corpus) >= 8 * MIB, "corpus size")
    require(cfg["working_set_bytes"] <= len(corpus), "working set exceeds corpus")
    working = cfg["working_set_bytes"]
    require(working >= 4 * MIB and working % BLOCK == 0, "working set alignment/size")
    trace = cfg.get("reads", [])
    require(cfg["phase"] not in ("read", "concurrent") or trace, "read trace required")
    for item in trace:
        require(item["size"] in (BLOCK, 4 * MIB) and item["offset"] >= 0 and item["offset"] % BLOCK == 0
                and item["offset"] + item["size"] <= working, "read trace bounds")
    with CreateClient(cfg["endpoint"], request_timeout=cfg["request_timeout_seconds"],
                      retry_timeout=0) as client:
        mounted = client.mount_volume(cfg["disk_id"], "")
        session = mounted["SessionInfo"].session_id
        require(mounted["Volume"].BlockSize == BLOCK, "mounted block size")
        require(mounted["Volume"].BlocksCount * BLOCK >= working, "disk too small")
        def effective_config(label):
            value = json.loads(client.execute_action(
                "getstorageconfig", json.dumps({"DiskId": cfg["disk_id"]}).encode()))
            for name, expected in cfg["expected_live_config"].items():
                require(field(value, name) == expected, "effective config mismatch: " + name)
            (args.output / (label + "-effective-config.json")).write_text(
                json.dumps(value, sort_keys=True, indent=2) + "\n")
            return value

        def describe(label):
            response = client.execute_action("describeblocks", json.dumps({
                "DiskId": cfg["disk_id"], "StartIndex": 0,
                "BlocksCount": working // BLOCK,
                "SupportedBlobFormatVersion": 1,
            }).encode())
            if isinstance(response, bytes):
                response = response.decode()
            result = json.loads(response)
            (args.output / (label + "-describe.json")).write_text(
                json.dumps(result, indent=2) + "\n")
            return result
        def operation(index, planned_ns):
            phase = cfg["phase"]
            write = phase.startswith("write") or (
                phase == "concurrent" and index % 100 < cfg["write_percentage"])
            if write:
                size = BLOCK if phase == "write-4k" else 4 * MIB
                # Concurrent read/write use the same immutable corpus bytes.
                # Every offset always has the same expected contents.
                offset = ((index * size) % (working - size + BLOCK)) // BLOCK * BLOCK
            else:
                item = trace[index % len(trace)]
                size, offset = item["size"], item["offset"]
            begin = time.monotonic_ns()
            error = None
            try:
                if write:
                    client.write_blocks(cfg["disk_id"], offset // BLOCK,
                                        [corpus[offset:offset + size]], session)
                else:
                    data = b"".join(client.read_blocks(
                        cfg["disk_id"], offset // BLOCK, size // BLOCK, "", session))
                    require(data == corpus[offset:offset + size], "read differs from corpus")
            except Exception as e:
                error = str(e)
            end = time.monotonic_ns()
            return {"index": index, "kind": "write" if write else "read",
                    "offset": offset, "size": size, "planned_ns": planned_ns,
                    "begin_ns": begin, "end_ns": end, "latency_ns": end - begin,
                    "queue_ns": max(0, begin - planned_ns), "error": error}
        try:
            initial_config = effective_config("before")
            before = describe("before")
            idle_start = time.monotonic_ns()
            idle_first = proc_sample(cfg["pids"])
            time.sleep(cfg["idle_seconds"])
            idle_last = proc_sample(cfg["pids"])
            idle_duration = (time.monotonic_ns() - idle_start) / 1e9
            idle_cpu = cpu_delta(idle_first, idle_last)
            (args.output / "idle-cpu.json").write_text(json.dumps({
                "first": idle_first, "last": idle_last, "seconds": idle_duration,
                "cpu_seconds": idle_cpu, "mean_busy_cores": idle_cpu / idle_duration,
            }, indent=2) + "\n")
            samples.append({"monotonic_ns": time.monotonic_ns(),
                            "proc": proc_sample(cfg["pids"]),
                            "metrics": metrics(cfg["metrics_url"]),
                            "net_dev": Path("/proc/net/dev").read_text(),
                            "network_namespaces": network_sample(cfg["pids"])})
            sampler.start()
            start = time.monotonic_ns()
            deadline = start + int(cfg["duration_seconds"] * 1e9)
            count = 0
            pending = set()
            with concurrent.futures.ThreadPoolExecutor(cfg["queue_depth"]) as pool:
                while time.monotonic_ns() < deadline:
                    planned = start + int(count * 1e9 / cfg["target_iops"])
                    delay = (planned - time.monotonic_ns()) / 1e9
                    if delay > 0:
                        time.sleep(min(delay, max(0, (deadline - time.monotonic_ns()) / 1e9)))
                    if time.monotonic_ns() >= deadline:
                        break
                    if len(pending) == cfg["queue_depth"]:
                        done, pending = concurrent.futures.wait(
                            pending, return_when=concurrent.futures.FIRST_COMPLETED)
                        operations.extend(f.result() for f in done)
                    pending.add(pool.submit(operation, count, planned))
                    count += 1
                operations.extend(f.result() for f in concurrent.futures.as_completed(pending))
            foreground_end = time.monotonic_ns()
            # A measured finite series includes all compaction it caused.
            # Explicit action names/arguments are from the isolated test setup;
            # no arbitrary shell command is run.
            if cfg["phase"] in ("write-4k", "concurrent"):
                require(cfg.get("drain"), "background drain action/predicate is required")
                action = cfg["drain"]
                response = client.execute_action(action["action"],
                    json.dumps(action["input"]).encode())
                (args.output / "drain-response.txt").write_text(str(response))
                status_input = dict(action["status_input"])
                if action.get("operation_id_field"):
                    status_input[action.get("status_operation_id_field", "OperationId")] = field(
                        json.loads(response), action["operation_id_field"])
                until = time.monotonic() + action["timeout_seconds"]
                while True:
                    result = json.loads(client.execute_action(action["status_action"],
                        json.dumps(status_input).encode()))
                    (args.output / "drain-status.json").write_text(json.dumps(result) + "\n")
                    # Protobuf JSON may omit false/default fields until done.
                    complete = next((v for k, v in result.items()
                                     if k.casefold() == action["completed_field"].casefold()), None)
                    if complete == action["completed_value"]:
                        break
                    require(time.monotonic() < until, "background drain timed out")
                    time.sleep(1)
            after = describe("after")
            require(effective_config("after") == initial_config, "effective config changed during run")
            require(process_identity(cfg["pids"]) == identity, "process identity changed during run")
            stop.set()
            sampler.join()
            end = time.monotonic_ns()
            samples.append({"monotonic_ns": end, "proc": proc_sample(cfg["pids"]),
                            "metrics": metrics(cfg["metrics_url"]),
                            "net_dev": Path("/proc/net/dev").read_text(),
                            "network_namespaces": network_sample(cfg["pids"])})
            require(samples and not sample_errors, "process/counter sampling failed: " + str(sample_errors))
            first, last = samples[0]["proc"], samples[-1]["proc"]
            cpu = cpu_delta(first, last)
            duration = (end - start) / 1e9
            success = [x for x in operations if x["error"] is None]
            logical = sum(x["size"] for x in success)
            summary = {"operations": len(operations), "errors": len(operations) - len(success),
                       "duration_seconds": duration,
                       "foreground_seconds": (foreground_end - start) / 1e9,
                       "achieved_iops": len(success) / duration,
                       "achieved_mib_s": logical / MIB / duration,
                       "cpu_seconds": cpu, "mean_busy_cores": cpu / duration,
                       "idle_mean_busy_cores": idle_cpu / idle_duration,
                       "cpu_seconds_above_idle": cpu - idle_cpu / idle_duration * duration,
                       "foreground_iops": len(success) / ((foreground_end - start) / 1e9),
                       "measurement_start_ns": start, "measurement_end_ns": end,
                       "cpu_seconds_per_operation": cpu / len(success) if success else None,
                       "cpu_seconds_per_gib": cpu / (logical / 1024**3) if logical else None,
                       "peak_rss_bytes": max(sum(v.get("VmRSS", 0) for v in s["proc"].values())
                                             for s in samples),
                       "latency_ns": {str(p): quantile([x["latency_ns"] for x in success], p)
                                      for p in (.50, .95, .99)},
                       "queue_ns": {str(p): quantile([x["queue_ns"] for x in operations], p)
                                    for p in (.50, .95, .99)},
                       "release_gate_passed": False}
            # Route/counter analysis and rollout budgets are separate checks.
            # A load generator exit code must never imply a release decision.
            (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            require(not summary["errors"], "operation errors; inspect raw evidence")
        finally:
            stop.set()
            if sampler.ident is not None:
                sampler.join()
            (args.output / "sampling-errors.json").write_text(json.dumps(sample_errors) + "\n")
            with (args.output / "samples.jsonl").open("w") as f:
                for x in sorted(samples, key=lambda x: x["monotonic_ns"]):
                    f.write(json.dumps(x) + "\n")
            with (args.output / "operations.jsonl").open("w") as f:
                for x in sorted(operations, key=lambda x: x["index"]):
                    f.write(json.dumps(x) + "\n")
            client.unmount_volume(cfg["disk_id"], session)
            corpus.close()
            corpus_file.close()


if __name__ == "__main__":
    main()
