#!/usr/bin/env bash

NODES="$1"

ls scenarios/*txt | while read p
do
    scenario_name=$(basename $p) &&
    echo "running ${scenario_name}" &&
    ./run_tests.sh "scenarios/${scenario_name}" "results/${scenario_name}" "$NODES" &&
    echo "finished ${scenario_name}" &&
    sleep 5
done
