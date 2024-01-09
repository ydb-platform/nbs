#! /usr/bin/env python3

# By a given list of dashboard ids, this script generates a list of metrics
# present on these dashboards. Used to generate a list of metrics, that are
# expected not to degrade over time. The nature of the metric if inferred from
# the title of the chart.
# Uses the `ycp` tool to fetch the dashboards content.
# Usage:
#   ./generate_metrics.py <base_url> <profile> <dashboard_id> [<dashboard_id> ...]

import subprocess
from sys import argv

from jinja2 import Template

import yaml
import json

matrics = []


def parse_solomon_dict(input_string):
    pairs = input_string.strip("{}").split(",")

    result_dict = {}

    for pair in pairs:
        key, value = pair.split("=")

        key = key.strip()
        value = value.strip(" '")

        result_dict[key] = value
    return result_dict


metrics = []

base_url = argv[1]
profile = argv[2]

for dashboard_id in argv[3:]:
    yaml_file = yaml.safe_load(
        subprocess.run(
            [
                "ycp",
                "--profile",
                profile,
                "monitoring",
                "v3",
                "dashboard",
                "get",
                "--id",
                dashboard_id,
            ],
            capture_output=True,
        ).stdout.decode("utf-8")
    )

    project_id = yaml_file["project_id"]

    vars = parse_solomon_dict(yaml_file["parametrization"]["selectors"])

    for line in yaml_file["widgets"]:
        title = line["chart"]["title"]
        queries = [
            Template(q["query"]).render(vars)
            for q in line["chart"]["queries"]["targets"]
        ]

        expected_to_grow = None
        if "iops" in title.lower():
            expected_to_grow = True
        elif "latency" in title.lower():
            expected_to_grow = False
        elif "bandwidth" in title.lower():
            expected_to_grow = True

        for query in queries:
            metric = {
                "dashboard_id": dashboard_id,
                "title": title,
                "query": query,
                "url": f"{base_url}/{dashboard_id}",
                "project_id": project_id,
            }
            if expected_to_grow is None or expected_to_grow == True:
                metric["expected_to_grow"] = True
                metrics.append(metric)
            if expected_to_grow is None or expected_to_grow == False:
                metric["expected_to_grow"] = False
                metrics.append(metric)

print(json.dumps(metrics, indent=4))
