#! /bin/bash

set -e

d="/root"
scripts="${d}/runner"
cluster="nemax"

results_path="/var/www/build/results/degradation_tests/${cluster}/$(date +%Y-%m-%d)"
mkdir -p "$results_path"

monitoring_url="https://monitoring.nebius.ai/projects/yc.nbs.cloud/dashboards"

dashboard_ids=(
    "aolabmsm1t9ab9t930td"
    "aolml7d7t6hepnc1hou4"
    "aolphv9oqpkufahuo7kh"
)

$scripts/degradation_test.py "$results_path" "$cluster" \
<($scripts/generate_metrics.py "$monitoring_url" "$cluster" "${dashboard_ids[@]}") \
2>> "$results_path/stderr.txt" >> "$results_path/stdout.txt"

$scripts/generate_generic_report.py degradation_tests $scripts/generic_report.xsl
