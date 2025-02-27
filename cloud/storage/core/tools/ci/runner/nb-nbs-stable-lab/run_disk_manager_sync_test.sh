#!/usr/bin/env bash
export instance_cores=4
export instance_ram=4

export disk_size_gib=2
export disk_block_size=4096
export disk_write_size_percentage=25
export test_suite="sync_2gib"

export cluster="nb-nbs-stable-lab"

scripts=$(dirname "$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )")
# shellcheck disable=SC1091
source "${scripts}/disk_manager_acceptance_common.sh"
run_sync
