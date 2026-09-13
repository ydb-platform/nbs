#!/usr/bin/env python3
"""Compare identical service workloads and old/new profile readers.

Run on the build host with binaries whose source provenance was verified.
This in-process workload measures splitter/stats/profile overhead, not gRPC or
device throughput. Every child output is retained; no builds are started here.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import subprocess
import time


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def assess_no_regression(summaries, repetitions):
    """Conservative one-sided bound; zero allowance, never pass noisy evidence.

    For n independent stationary pairs, max(delta) bounds the median paired
    degradation with error probability at most 2**-n. A union bound covers all
    scenario/metric comparisons. This is not a bound for every I/O request.
    """
    results = []
    for summary in summaries:
        for metric in ("p50_us", "p95_us", "p99_us", "operations_per_second"):
            pairs = summary["metrics"][metric]["pairs"]
            if len(pairs) != repetitions:
                raise ValueError("Incomplete performance pairs")
            direction = -1 if metric == "operations_per_second" else 1
            deltas = []
            bounds = []
            for pair in pairs:
                before, after = pair["before"], pair["after"]
                if not all(math.isfinite(x) and x >= 0 for x in (before, after)):
                    raise ValueError("Invalid performance observation")
                delta = direction * (after - before)
                # The workload prints nine decimals. Include rounding in the
                # upper bound; this is uncertainty, not an allowed regression.
                uncertainty = 1e-9 + 4 * math.ldexp(1.0, max(
                    -1074, math.frexp(max(before, after))[1] - 53))
                deltas.append(delta)
                bounds.append(delta + uncertainty)
            upper = max(bounds)
            status = ("insufficient_repetitions" if repetitions < 10 else
                      "passed" if upper <= 0 else
                      "observed_regression" if min(deltas) > 0 else
                      "inconclusive")
            results.append({
                "scenario": summary["scenario"], "metric": metric,
                "degradation_direction": direction,
                "median_degradation": statistics.median(deltas),
                "upper_bound": upper, "allowed_degradation": 0,
                "status": status,
            })
    passed = bool(results) and all(x["status"] == "passed" for x in results)
    status = ("passed" if passed else "observed_regression" if any(
        x["status"] == "observed_regression" for x in results) else "inconclusive")
    return {
        "passed": passed,
        "status": status,
        "metrics": results,
        "simultaneous_confidence_lower_bound": max(
            0, 1 - len(results) * 2.0 ** -repetitions),
        "assumptions": "independent paired trials with stationary conditions",
        "scope": "median paired change of per-run quantiles and throughput",
        "network_disk_acceptance": "requires a separate representative network path",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("before", "after", "old-reader", "new-reader", "output"):
        parser.add_argument("--" + option, required=True, type=Path)
    parser.add_argument("--iterations", type=int, default=256)
    parser.add_argument("--repetitions", type=int, default=10)
    args = parser.parse_args()
    if args.iterations < 1 or args.repetitions < 10:
        parser.error("iterations must be positive and repetitions at least ten")
    binaries = {
        name: getattr(args, name.replace("-", "_")).resolve(strict=True)
        for name in ("before", "after", "old-reader", "new-reader")
    }
    # A new directory prevents accidental mixing of trials or profile records.
    output = args.output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "scope": "in-process splitter + server stats + memory backend + profile writer",
        "binaries": {k: {"path": str(v), "sha256": sha256(v)}
                     for k, v in binaries.items()},
        "iterations": args.iterations,
        "repetitions": args.repetitions,
        "minimum_measured_operations_per_trial": 10000,
        "scenario_iterations": [],
        "invocations": [],
        "compatibility": [],
        "trials": [],
    }

    def save():
        (output / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    def run(name, command):
        stdout = output / (name + ".stdout")
        stderr = output / (name + ".stderr")
        record = {"name": name, "argv": list(map(str, command)),
                  "started_unix": time.time()}
        manifest["invocations"].append(record)
        save()
        with stdout.open("wb") as out, stderr.open("wb") as err:
            result = subprocess.run(command, stdout=out, stderr=err)
        record.update(exit_code=result.returncode, finished_unix=time.time(),
                      stdout_sha256=sha256(stdout), stderr_sha256=sha256(stderr))
        save()
        if result.returncode:
            raise RuntimeError(f"{name} failed: exit {result.returncode}; see {stderr}")
        return stdout.read_text()

    def workload(name, version, scenario, iterations, error_part=-1):
        operation, size, parts, concurrency, wait_us = scenario
        profile = output / (name + ".profile")
        text = run(name, [binaries[version], profile, operation, str(size),
                          str(parts), str(concurrency), str(iterations),
                          str(wait_us), str(error_part)])
        data = json.loads(text)
        expected = concurrency * iterations
        if data["operations"] != expected or data["backend_calls"] != expected * parts:
            raise RuntimeError(f"{name}: operation/split count mismatch")
        expected_errors = expected if error_part >= 0 else 0
        if (data["error_count"] != expected_errors or
                data["expected_errors"] != expected_errors):
            raise RuntimeError(f"{name}: unexpected response errors")
        warmup = min(iterations, 20) * concurrency
        if (data["warmup_operations"] != warmup or
                data["profile_records_total"] != expected + warmup):
            raise RuntimeError(f"{name}: warmup/profile count mismatch")
        if not profile.is_file() or not profile.stat().st_size:
            raise RuntimeError(f"{name}: missing production profile log")
        expected_mib = data["operations_per_second"] * size / 1048576.0
        if not math.isclose(
                data["mib_per_second"], expected_mib,
                rel_tol=1e-12, abs_tol=1e-7):
            raise RuntimeError(f"{name}: IOPS/throughput arithmetic differs")
        return data, profile

    # Small fixtures cover both operations, ordinary/composite, success/error,
    # complete/unlocated timing, without depending on measurements for an oracle.
    fixtures = [
        (("read", 4096, 1, 1, 0), -1, True),
        (("write", 65536, 4, 1, 0), -1, True),
        (("read", 65536, 4, 1, 20), 1, True),
        (("write", 4096, 1, 1, -1), -1, False),
        (("read", 4096, 1, 1, 1000), -1, True),
    ]
    for index, (scenario, error_part, expected_complete) in enumerate(fixtures):
        name = f"compat-{index}"
        data, profile = workload(name, "after", scenario, 2, error_part)
        old = run(name + "-old", [binaries["old-reader"], profile])
        new = run(name + "-new", [binaries["new-reader"], profile])
        if old != new:
            raise RuntimeError(f"{name}: previous reader and new default output differ")
        legacy_rows = [line.split("\t") for line in old.splitlines()]
        if not legacy_rows or any(len(row) != 7 or row[4] != "R"
                                  for row in legacy_rows):
            raise RuntimeError(f"{name}: unexpected legacy output shape")
        timing_text = run(name + "-timing",
                          [binaries["new-reader"], "--request-timing", profile])
        rows = [json.loads(line) for line in timing_text.splitlines()]
        if len(rows) != data["profile_records_total"] or len(legacy_rows) != len(rows):
            raise RuntimeError(f"{name}: profile record loss")
        for row in rows:
            timing = row["timing"]
            if timing.get("version") != 1 or timing["total_us"] != row["duration_us"]:
                raise RuntimeError(f"{name}: absent or inconsistent diagnostic field")
            if (timing["complete"] is not expected_complete or
                    timing["selected_categories"] != 7):
                raise RuntimeError(f"{name}: unexpected completeness or categories")
            if bool(timing["error_code"]) != (error_part >= 0):
                raise RuntimeError(f"{name}: diagnostic response outcome differs")
            total = timing["total_us"]
            if type(total) is not int or total < 0:
                raise RuntimeError(f"{name}: invalid total duration")
            without = timing["without_waits_us"]
            impact = timing["wait_impact_us"]
            if expected_complete:
                if (type(without) is not int or type(impact) is not int or
                        not 0 <= without <= total or impact != total - without or
                        timing["reason"]):
                    raise RuntimeError(f"{name}: invalid complete calculation")
                if scenario[4] == 0 and (without != total or impact != 0):
                    raise RuntimeError(f"{name}: no-wait request lost service time")
                # A single waited part has no sibling that could hide its wait.
                # Check the sign, not equality to the requested sleep duration.
                if scenario[2] == 1 and scenario[4] > 0 and impact <= 0:
                    raise RuntimeError(f"{name}: known wait has no measured impact")
            else:
                if without is not None or impact is not None or not timing["reason"]:
                    raise RuntimeError(f"{name}: incomplete result must have nulls/reason")
                if not any(stage["missing_categories"] & 4 and
                           stage["unlocated_wait_us"][2] > 0
                           for stage in timing["stages"]):
                    raise RuntimeError(f"{name}: missing Shaping evidence was lost")
        manifest["compatibility"].append({
            "scenario": scenario, "error_part": error_part,
           "expected_complete": expected_complete,
            "records": len(rows), "profile_sha256": sha256(profile),
            "old_default_equals_new_default": True})
        save()

    shapes = [(4096, 1, 1, 0), (65536, 1, 32, 0),
              (1048576, 4, 1, 0), (4194304, 16, 32, 0),
              (1048576, 4, 32, 100)]
    metrics = ("p50_us", "p95_us", "p99_us", "operations_per_second",
               "mib_per_second", "user_cpu_seconds", "system_cpu_seconds",
               "peak_rss_kib", "drain_seconds", "wall_seconds")
    summaries = []
    for operation in ("read", "write"):
        for shape in shapes:
            scenario = (operation, *shape)
            iterations = max(
                args.iterations, (10000 + shape[2] - 1) // shape[2])
            manifest["scenario_iterations"].append({
                "scenario": scenario, "iterations_per_lane": iterations})
            paired = {metric: [] for metric in metrics}
            for trial in range(args.repetitions):
                versions = ("before", "after") if trial % 2 == 0 else ("after", "before")
                results = {}
                for version in versions:
                    name = f"{operation}-{shape[0]}-{shape[1]}-{shape[2]}-{shape[3]}-{trial}-{version}"
                    data, profile = workload(name, version, scenario, iterations)
                    results[version] = data
                    manifest["trials"].append({
                        "scenario": scenario, "trial": trial, "version": version,
                        "metrics": data, "profile_sha256": sha256(profile)})
                    save()
                for metric in metrics:
                    before, after = results["before"][metric], results["after"][metric]
                    paired[metric].append({
                        "before": before, "after": after,
                        "delta": after - before,
                        "percent": (after / before - 1) * 100 if before else None})
            summaries.append({
                "scenario": scenario,
                "metrics": {
                    metric: {"pairs": pairs,
                             "median_delta": statistics.median(p["delta"] for p in pairs),
                             "min_delta": min(p["delta"] for p in pairs),
                             "max_delta": max(p["delta"] for p in pairs)}
                    for metric, pairs in paired.items()}})
    # Recheck tools to catch accidental replacement of mutable build outputs.
    for name, binary in binaries.items():
        if sha256(binary) != manifest["binaries"][name]["sha256"]:
            raise RuntimeError(f"{name}: binary changed during comparison")
    manifest["completed"] = True
    manifest["summary"] = summaries
    manifest["performance_acceptance"] = assess_no_regression(
        summaries, args.repetitions)
    save()
    passed = manifest["performance_acceptance"]["passed"]
    print(json.dumps({
        "completed": True, "performance_passed": passed,
        "manifest": str(output / "manifest.json")}))
    if not passed:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
