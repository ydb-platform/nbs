#!/usr/bin/env bash
export instance_cores=4
export instance_ram=4

export test_suite="medium"
source "${0%/*}/disk_manager_acceptance_common.sh"
run_acceptance
