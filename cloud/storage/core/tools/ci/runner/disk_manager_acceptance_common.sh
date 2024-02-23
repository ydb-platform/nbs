#!/usr/bin/env bash
export d="/root"
scripts=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export scripts
export dm="${d}/disk_manager_acceptance_test/disk_manager_acceptance_tests"
export cluster="nemax"

export results_dir="/var/www/build/results"

function create_results_directory () {
    rm -rf "$results_path"
    mkdir -p "$results_path"
}

function base_shell_args () {
    result=(
        "--no-generate-ycp-config"
        "--ycp-requests-template-path" "$dm/templates/"
        "--ssh-key-path" "/root/.ssh/test-ssh-key"
        "--profile-name" "${cluster}-tests"
        "--cluster-config-path" "$dm/configs/"
        "--cluster" "${cluster}"
        "--zone-ids" "eu-north1-a"
        "--instance-cores" "${instance_cores:?Variable instance_cores not present}"
        "--instance-ram" "${instance_ram:?Variable instance_ram not present}"
        "--chunk-storage-type" "${chunk_storage_type:=ydb}"
        "--instance-platform-ids" "standard-v2" "standard-v3"
        "--cleanup-before-tests"
        "--acceptance-test" "$dm/acceptance-test"
        "--results-path" "${results_path}"
        "--verbose"
    )
    echo "${result[@]}"
}

function report_results () {
    "$scripts/generate_generic_report.py" "disk_manager_${test_name:=acceptance}" "$scripts/generic_report.xsl"
}

function execute_tests () {
    export result_case_directory="${results_dir}/disk_manager_${test_name:=acceptance}/${cluster}/disk-manager/"
    results_path="${result_case_directory}${test_suite:?"test_suite parameter undefined"}/$(date +%Y-%m-%d)"
    export results_path
    create_results_directory
    export lockfile="/tmp/disk_manager_${test_name:=acceptance}_${test_suite:?"test_suite parameter undefined"}.lock"
    # shellcheck disable=SC2068
    # shellcheck disable=SC2046
    (
      flock 200
      $dm/disk-manager-ci-acceptance-test-suite $(base_shell_args) $@ \
          2>> "$results_path/stderr.txt" \
          >> "$results_path/stdout.txt"
    ) 200>"$lockfile"
    report_results
}

function run_acceptance () {
    export test_name="acceptance"
    execute_tests \
    acceptance \
    --test-suite "${test_suite:?Test suite is undefined}" \
    --verify-test "$dm/verify-test" \
    --s3-host "storage.nemax.nebius.cloud"
}

function run_eternal () {
    export test_name="eternal"
    execute_tests \
    --conserve-snapshots \
    eternal \
    --disk-size "${disk_size_gib:?disk_size_gib is not defined}" \
    --disk-blocksize "${disk_block_size:?disk_block_size is not defined}" \
    --disk-type network-ssd \
    --disk-write-size-percentage "${disk_write_size_percentage:?disk_write_size_percentage}" \
    --cmp-util "$dm/acceptance-cmp"
}

function run_sync () {
    export test_name="sync"
    execute_tests \
    --conserve-snapshots \
    sync \
    --disk-size "${disk_size_gib:?"disk_size_gib is not defined"}" \
    --disk-blocksize "${disk_block_size:?"disk_block_size is not defined"}" \
    --disk-type network-ssd \
    --disk-write-size-percentage "${disk_write_size_percentage:?"disk_write_size_percentage"}"
}
