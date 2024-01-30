#!/usr/bin/env python3

# This test validates that the results of the fio test cases as well as the
# eternal test performance are not degrading over time.
# Fio test performance is obtained from the fio test history, while the eternal
# test performance is obtained from the monitoring system.

import datetime
import json
import os
import subprocess
import sys
import tempfile
import time
import typing as tp
from functools import partial

import yaml

from generate_fio_report import on_test_case_result_impl
from report_common import ROOT_DIR, build_report

# If the performance of a test case degrades by this factor, it is considered a
# degradation. For metrics that are supposed to be as low as possible (e.g.
# latency), the factor is inverted.
DEGRADATION_FACTOR = 0.8

# For degradation tests, we compare performance over `LAST_PERIOD_DAYS` against
# the performance over `OVERALL_PERIOD_DAYS`.
OVERALL_PERIOD_DAYS = 14
LAST_PERIOD_DAYS = 5

# Use median as the aggregate function for the test case results.
AGGREGATE_FUNCTION = lambda x: sorted(x)[len(x) // 2] if len(x) > 0 else 0


T = tp.TypeVar("T")


def extract_metrics_values(
    history: tp.Sequence[T],
    data_getter: tp.Callable[[T], float],
    date_getter: tp.Callable[[T], datetime.date],
    period_days: int,
) -> float:
    return AGGREGATE_FUNCTION(
        [
            data_getter(x)
            for x in history
            if date_getter(x)
            >= datetime.datetime.now().date()
            - datetime.timedelta(days=period_days)
        ]
    )


def generate_metrics_report(
    history: tp.Sequence[T],
    data_getter: tp.Callable[[T], float],
    date_getter: tp.Callable[[T], datetime.date],
    expected_to_grow: bool,
    name: str,
    date: datetime.date,
    description: str,
) -> tp.Dict[str, tp.Any]:
    """
    Generate a metrics degradation report based on the given history.
    """
    overall = extract_metrics_values(
        history, data_getter, date_getter, OVERALL_PERIOD_DAYS
    )
    last = extract_metrics_values(
        history, data_getter, date_getter, LAST_PERIOD_DAYS
    )
    degraded = (
        last < overall * DEGRADATION_FACTOR
        if expected_to_grow
        else last > overall / DEGRADATION_FACTOR
    )
    improved = (
        last > overall / DEGRADATION_FACTOR
        if expected_to_grow
        else last < overall * DEGRADATION_FACTOR
    )

    result = {
        f"last {OVERALL_PERIOD_DAYS} days": overall,
        f"last {LAST_PERIOD_DAYS} days": last,
        "degraded": str(degraded),
        "improved": str(improved),
        "status": "fail" if degraded else ("ok" if not improved else "improved"),
        "name": name,
        "date": str(date),
        "description": description,
    }

    if degraded:
        result["error_message"] = f"Performance degraded"
    if improved:
        result["highlight_color"] = "#ffffcc"

    return result


def generate_reports_local(
    suite_kinds: tp.Sequence[str],
    reports: list[tp.Dict[str, tp.Any]],
    cluster: str,
) -> None:
    for suite_kind in suite_kinds:
        results_path = os.path.join(ROOT_DIR, "results", suite_kind, cluster)

        by_all_time = {}

        build_report(
            suite_kind,
            partial(on_test_case_result_impl, by_all_time=by_all_time),
            results_path,
        )

        for test_tag, history in by_all_time.items():
            for getter, sensor_name in [
                (lambda x: x.read_iops, "read_iops"), 
                (lambda x: x.write_iops, "write_iops"), 
                (lambda x: x.read_bw, "read_bw"), 
                (lambda x: x.write_bw, "write_bw") 
            ]:
                report = generate_metrics_report(
                    history.points,
                    getter,
                    lambda x: datetime.datetime.strptime(
                        x.date, "%Y-%m-%d"
                    ).date(),
                    expected_to_grow=True,
                    name=test_tag,
                    date=today,
                    description=f"{suite_kind} {test_tag} {sensor_name}",
                )
                reports.append(report)


def get_monitoring_results(
    query: str,
    datapoints: int,
    from_timestamp: datetime.datetime,
    to_timestamp: datetime.datetime,
    profile: str,
    project_id: str,
) -> list[list[tp.Tuple[datetime.datetime, float]]]:
    request = {
        "container": {
            "project_id": project_id,
        },
        "queries": [{"name": "sensor", "value": query}],
        "downsampling": {
            "max_points": datapoints,
            "grid_aggregation": "GRID_AGGREGATION_AVG",
            "gap_filling": "GAP_FILLING_PREVIOUS",
        },
        "from_time": from_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "to_time": to_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
    with tempfile.NamedTemporaryFile() as f:
        f.write(yaml.dump(request).encode("utf-8"))
        f.flush()

        result = subprocess.run(
            [
                "ycp",
                "--profile",
                profile,
                "monitoring",
                "v3",
                "data",
                "read",
                "-r",
                f.file.name,
            ],
            capture_output=True,
        )
        time.sleep(0.1)
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        responses = yaml.safe_load(result.stdout)["response_per_query"][0][
            "timeseries_vector"
        ]["values"]
        result = []
        for response in responses:
            if "timestamp_values" not in response:
                continue
            timestamps = list(
                map(
                    lambda x: datetime.datetime.utcfromtimestamp(int(x) / 1000),
                    response["timestamp_values"]["values"],
                )
            )
            if "double_values" not in response:
                continue
            values = response["double_values"]["values"]
            result.append(list(zip(timestamps, values)))
        return result


def generate_reports_monitoring(
    profile: str,
    reports: list[tp.Dict[str, tp.Any]],
    sensors: list[tp.Dict[str, tp.Any]],
) -> None:
    for sensor in sensors:
        print(sensor)

        results = get_monitoring_results(
            query=sensor["query"],
            datapoints=100,
            from_timestamp=datetime.datetime.now()
            - datetime.timedelta(days=OVERALL_PERIOD_DAYS),
            to_timestamp=datetime.datetime.now(),
            profile=profile,
            project_id=sensor["project_id"],
        )

        for result in results:
            report = generate_metrics_report(
                result,
                lambda x: x[1],
                lambda x: x[0].date(),
                expected_to_grow=sensor["expected_to_grow"],
                name=sensor["title"],
                date=today,
                description=f"{sensor['url']} {sensor['query']}",
            )
            reports.append(report)


if __name__ == "__main__":
    output_dir = sys.argv[1]
    cluster = sys.argv[2]
    profile = sys.argv[3]
    sensors = json.loads(open(sys.argv[4]).read())

    today = datetime.datetime.now().date()

    reports = []

    generate_reports_local(["fio", "nfs_fio"], reports, cluster)
    generate_reports_monitoring(profile, reports, sensors)

    reports.sort(key=lambda x: x.get("status", "ok"))

    with open(os.path.join(output_dir, "degradation.json"), "w") as f:
        json.dump(reports, f, indent=4)
