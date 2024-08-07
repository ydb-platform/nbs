#!/usr/bin/env bash

d="/root"
scripts="${d}/runner"
cluster="nemax"

function run_test () {
    test_suite=$1
    shift
    results_path="/var/www/build/results/corruption/${cluster}/nbs/${test_suite}/$(date +%Y-%m-%d)"
    mkdir -p "$results_path"

    # shellcheck disable=SC2068
    $d/yc-nbs-ci-corruption-test-suite --test-suite "$test_suite" $@ --cluster $cluster --profile "${cluster}-tests" --zone-id eu-north1-a --ssh-key-path /root/.ssh/test-ssh-key \
        --no-generate-ycp-config --results-path "$results_path" --cluster-config-path $d/fio_dep/cluster-configs \
        --ycp-requests-template-path $d/fio_dep/ycp-request-templates --verify-test-path $d/verify-test 2>> "$results_path/stderr.txt" >> "$results_path/stdout.txt"
}

run_test ranges-intersection
run_test 64MB-bs
run_test 512bytes-bs

$scripts/generate_generic_report.py corruption $scripts/generic_report.xsl
