#!/usr/bin/env bash
export instance_cores=8
export instance_ram=8

export test_suite="enormous"
source "${0%/*}/disk_manager_acceptance_common.sh"
run_acceptance
