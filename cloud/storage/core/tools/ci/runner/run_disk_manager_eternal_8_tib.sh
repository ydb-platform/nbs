#!/usr/bin/env bash
export instance_cores=24
export instance_ram=24

export disk_size_gib=8192
export disk_block_size=4096
export disk_write_size_percentage=1
export test_suite="eternal_8tib"

scripts=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
# shellcheck disable=SC1091
source "${scripts}/disk_manager_acceptance_common.sh"
run_eternal
