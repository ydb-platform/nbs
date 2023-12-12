#!/usr/bin/env bash

d="/root"
scripts="${d}/runner"
cluster="nemax"

function run_test () {
    test_suite=$1
    shift
    results_path="/var/www/build/results/nfs_fio/${cluster}/nfs/${test_suite}/$(date +%Y-%m-%d)"
    mkdir -p $results_path

    $d/yc-nbs-ci-fio-performance-test-suite --test-suite $test_suite --service nfs $@ --cluster $cluster --profile-name "${cluster}-tests" --zone-id eu-north1-a --instance-cores 16 --instance-ram 16 --ssh-key-path /root/.ssh/test-ssh-key \
        --no-generate-ycp-config --results-path $results_path --cluster-config-path $d/fio_dep/cluster-configs \
        --ycp-requests-template-path $d/fio_dep/ycp-request-templates 2>> $results_path/stderr.txt >> $results_path/stdout.txt
}

run_test default_all_types

$scripts/generate_fio_report.py $scripts/fio.xsl nfs_fio
