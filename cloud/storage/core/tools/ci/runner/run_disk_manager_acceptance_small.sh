#!/usr/bin/env bash
export instance_cores=2
export instance_ram=2

export test_suite="small"
source "${0%/*}/disk_manager_acceptance_common.sh"
run_acceptance
