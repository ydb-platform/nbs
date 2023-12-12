#!/usr/bin/env bash

d="/root"
scripts="${d}/runner"
cluster="nemax"

function run_test () {
    zone=$1
    shift
    results_path="/var/www/build/results/check_emptiness/${cluster}/nbs/emptiness_${zone}/$(date +%Y-%m-%d)"
    mkdir -p $results_path

    $d/yc-nbs-ci-check-nrd-disk-emptiness-test $@ --cluster $cluster --profile-name "${cluster}-tests" --zones $zone --ssh-key-path /root/.ssh/test-ssh-key \
        --no-generate-ycp-config --results-path $results_path --cluster-config-path $d/fio_dep/cluster-configs \
        --ycp-requests-template-path $d/fio_dep/ycp-request-templates --verify-test-path $d/verify-test 2>> $results_path/stderr.txt >> $results_path/stdout.txt
}

run_test eu-north1-a
run_test eu-north1-b
run_test eu-north1-c

$scripts/generate_generic_report.py check_emptiness $scripts/generic_report.xsl
