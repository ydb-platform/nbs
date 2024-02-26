#!/usr/bin/env bash
export instance_cores=4
export instance_ram=4

export test_suite="medium"

export cluster="nb-nbs-stable-lab"

scripts=$(dirname "$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )")
# shellcheck disable=SC1091
source "${scripts}/disk_manager_acceptance_common.sh"
run_acceptance
