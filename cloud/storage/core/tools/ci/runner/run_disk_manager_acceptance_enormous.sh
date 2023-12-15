#!/usr/bin/env bash
export instance_cores=8
export instance_ram=8

export test_suite="enormous"

scripts=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
# shellcheck disable=SC1091
source "${scripts}/disk_manager_acceptance_common.sh"
run_acceptance
