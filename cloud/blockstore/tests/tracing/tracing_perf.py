"""Paired before/after measurements including NBS background trace export."""
import concurrent.futures
import json
import os
from pathlib import Path
import threading
import time
import urllib.request

import yatest.common as common
from cloud.storage.core.protos import media_pb2
from cloud.blockstore.tests.tracing.tracing_harness import Environment, sha, utc, write_json


def process_sample(pid):
    # /proc fields after comm start at state (field 3).
    fields = Path("/proc/%s/stat" % pid).read_text().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return dict(pid=pid, start_ticks=int(fields[19]), cpu_seconds=(int(fields[11]) + int(fields[12])) / ticks,
                rss_bytes=int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
                monotonic=time.monotonic())


def percentiles(values):
    values = sorted(values)
    assert values
    return {name: values[min(len(values) - 1, int((len(values) - 1) * p))]
            for name, p in (("p50", .5), ("p95", .95), ("p99", .99), ("p999", .999))}


def phase(env, disk, session, operation, blocks, depth, seconds, raw_path=None):
    latencies = []
    errors = []
    lock = threading.Lock()
    begin = time.monotonic()
    deadline = begin + seconds

    def worker(worker_id):
        local = []
        try:
            while time.monotonic() < deadline:
                request = env.operation(
                    operation, disk, session, index=worker_id * 256, blocks=blocks)
                env.header(request)
                started = time.monotonic_ns()
                response = getattr(env.client, operation)(request, timeout=60)
                assert not response.Error.Code & 0x80000000, response
                local.append((time.monotonic_ns() - started) / 1000)
        except BaseException as error:
            with lock:
                errors.append(repr(error))
        finally:
            with lock:
                latencies.extend(local)

    with concurrent.futures.ThreadPoolExecutor(max_workers=depth) as pool:
        list(pool.map(worker, range(depth)))
    elapsed = time.monotonic() - begin
    assert not errors, errors
    assert latencies
    if raw_path is not None:
        write_json(raw_path, latencies)
    return dict(
        elapsed_seconds=elapsed, requests=len(latencies),
        bytes=len(latencies) * blocks * 4096,
        iops=len(latencies) / elapsed,
        bytes_per_second=len(latencies) * blocks * 4096 / elapsed,
        latency_us=percentiles(latencies), errors=errors)


def measure(output, binary, media, operation, blocks, depth, seconds, warmup):
    env = Environment(output, nbs_binary=binary, sampling=1000, viewer=False, throttling=False)
    samples = []
    sampler_errors = []
    stop = threading.Event()
    sampler = None
    try:
        disk = "trace-perf"
        session = env.volume(media, disk)
        # Materialize all read ranges in BlobStorage, not as implicit zeroes.
        for worker in range(depth):
            env.call("WriteBlocks", env.operation(
                "WriteBlocks", disk, session, index=worker * 256, blocks=256))
        phase(env, disk, session, operation, blocks, depth, warmup)

        counter_url = "http://127.0.0.1:%s/counters/counters=blockstore/json" % env.nbs.mon_port

        def capture_counters(label):
            raw = urllib.request.urlopen(counter_url, timeout=10).read()
            json.loads(raw)
            Path(output, "counters-%s.json" % label).write_bytes(raw)
            with env.collector.lock:
                return dict(label=label, at=utc(), monotonic=time.monotonic(),
                            collector_exports=len(env.collector.receipts),
                            collector_spans=sum(r["spans"] for r in env.collector.receipts))

        processes = {"nbs": env.nbs.pid, "harness": os.getpid()}
        processes.update({"ydb-%s" % key: node.pid
                          for key, node in env.cluster.nodes.items()})
        assert len(processes) > 2 and len(set(processes.values())) == len(processes)

        def snapshot():
            return {role: process_sample(pid) for role, pid in processes.items()}

        counters = [capture_counters("before")]

        def sample():
            try:
                while not stop.wait(.2):
                    samples.append(snapshot())
            except Exception as error:
                sampler_errors.append(repr(error))

        first = snapshot()
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        measured = phase(env, disk, session, operation, blocks, depth, seconds,
                         Path(output, "latencies-us.json"))
        counters.append(capture_counters("after-io"))
        # Include the background export drain and its time series in accounting.
        for index in range(15):
            time.sleep(2)
            counters.append(capture_counters("drain-%s" % (index + 1)))
        last = snapshot()
        stop.set()
        sampler.join(5)
        assert not sampler.is_alive(), "Process sampler did not stop"
        assert not sampler_errors, sampler_errors
        write_json(Path(output, "counter-observations.json"), counters)
        all_samples = [first] + samples + [last]
        for role in processes:
            assert all(row[role]["start_ticks"] == first[role]["start_ticks"]
                       for row in all_samples), ("Process identity changed", role)
        cpu = {role: last[role]["cpu_seconds"] - first[role]["cpu_seconds"]
               for role in processes}
        peak_rss = {role: max(row[role]["rss_bytes"] for row in all_samples)
                    for role in processes}
        ydb_roles = [role for role in processes if role.startswith("ydb-")]
        write_json(Path(output, "process-accounting.json"), dict(
            processes=processes, first=first, last=last,
            cpu_seconds=cpu, peak_rss_bytes=peak_rss,
            harness_scope="Load generator, OTLP collector and process sampler"))
        measured.update(
            nbs_cpu_seconds=cpu["nbs"],
            nbs_peak_rss_bytes=peak_rss["nbs"],
            ydb_cpu_seconds=sum(cpu[role] for role in ydb_roles),
            ydb_peak_rss_bytes=max(sum(row[role]["rss_bytes"] for role in ydb_roles)
                                   for row in all_samples),
            harness_cpu_seconds=cpu["harness"],
            harness_peak_rss_bytes=peak_rss["harness"],
            accounting_seconds=last["nbs"]["monotonic"] - first["nbs"]["monotonic"],
            background_drain_seconds=30,
            collector_exports=len(env.collector.receipts),
            collector_spans=sum(r["spans"] for r in env.collector.receipts))
        write_json(Path(output, "measurement.json"), measured)
        return measured
    finally:
        stop.set()
        if sampler:
            sampler.join(5)
        write_json(Path(output, "process-samples.json"), samples)
        env.close()


def run_comparison(output, baseline):
    candidate = common.get_param("tracing_candidate")
    assert candidate and Path(candidate).is_absolute() and Path(candidate).is_file(), (
        "tracing_candidate must name the pinned candidate release binary")
    output.mkdir(parents=True, exist_ok=False)
    seconds = int(common.get_param("tracing_perf_seconds", "30"))
    warmup = int(common.get_param("tracing_perf_warmup", "10"))
    repeats = int(common.get_param("tracing_perf_repeats", "3"))
    assert seconds >= 10 and warmup >= 5 and repeats >= 1
    profile_filter = common.get_param("tracing_perf_profile")
    valid_profiles = {
        "%s-%s-%sblocks-qd%s" % (media, operation, blocks, depth)
        for media in ("ssd", "hdd")
        for operation in ("ReadBlocks", "WriteBlocks")
        for blocks, depth in ((1, 1), (256, 32))}
    assert profile_filter in valid_profiles, (
        "tracing_perf_profile must select one of the eight complete paired profiles",
        sorted(valid_profiles))
    rows = []
    write_json(output / "inputs.json", dict(
        baseline=dict(path=baseline, sha256=sha(baseline)),
        candidate=dict(path=candidate, sha256=sha(candidate)),
        seconds=seconds, warmup=warmup, repeats=repeats, sampling=1000,
        profile=profile_filter, background_drain_seconds=30,
        cpu_scope="NBS, all YDB nodes, and load/collector harness; includes background drain",
        media_scope="YDB SSD/HDD pools backed by in-memory test PDisks",
        acceptance="Per-pair deltas are evidence; threshold is not inferred.",
        started_at=utc()))
    for media, media_name in ((media_pb2.STORAGE_MEDIA_SSD, "ssd"),
                              (media_pb2.STORAGE_MEDIA_HDD, "hdd")):
        for operation in ("ReadBlocks", "WriteBlocks"):
            for blocks, depth in ((1, 1), (256, 32)):
                profile = "%s-%s-%sblocks-qd%s" % (media_name, operation, blocks, depth)
                if profile != profile_filter:
                    continue
                for repeat in range(repeats):
                    values = {}
                    order = (("baseline", baseline), ("candidate", candidate))
                    if repeat % 2:
                        order = tuple(reversed(order))
                    for name, binary in order:
                        directory = output / ("%s-%s-%s" % (profile, repeat, name))
                        values[name] = measure(
                            directory, binary, media, operation, blocks, depth,
                            seconds, warmup)
                    before, after = values["baseline"], values["candidate"]
                    delta = {key: after[key] / before[key] - 1
                             for key in ("iops", "bytes_per_second",
                                         "nbs_cpu_seconds", "nbs_peak_rss_bytes",
                                         "ydb_cpu_seconds", "ydb_peak_rss_bytes",
                                         "harness_cpu_seconds", "harness_peak_rss_bytes")
                             if before[key]}
                    delta["p99_latency_us"] = (
                        after["latency_us"]["p99"] / before["latency_us"]["p99"] - 1)
                    rows.append(dict(profile=profile, repeat=repeat, values=values,
                                     relative_change=delta))
                    write_json(output / "paired-results.json", rows)
    assert len(rows) == repeats
    write_json(output / "completion.json", dict(
        completed_at=utc(), pairs=len(rows), all_pairs_have_successful_io=True))
