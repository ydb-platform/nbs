#!/usr/bin/env bash
export instance_cores=4
export instance_ram=4

export disk_size_gib=8
export disk_block_size=4096
export disk_write_size_percentage=25
export test_suite="eternal_8gib"
source "${0%/*}/disk_manager_acceptance_common.sh"
run_eternal
