#!/usr/bin/env bash
export instance_cores=2
export instance_ram=2

export test_suite="small"

export cluster="nb-nbs-stable-lab"

scripts=$(dirname "$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )")
# shellcheck disable=SC1091
source "${scripts}/disk_manager_acceptance_common.sh"
run_acceptance
