import logging
import os
import re

import yatest.common as common


def setup_coredumps(ssh):
    ssh("sudo mkdir /cores")
    ssh("sudo sysctl -w 'kernel.core_pattern=/cores/%E.%p'")

    # Default core limit is 0, so it's essential to increase it. Unfortunately
    # there seems to be no portable way to do this across all images used in
    # our tests. What works every time, is setting them locally inside the
    # /run_test.sh script

    gdb = common.runtime.gdb_path()
    gdb_args = "--batch -iex 'set print thread-events off' -iex 'set auto-load safe-path /' -ex bt"
    backtrace_dir = common.output_path()
    script = "\n".join([
        f"#!/bin/bash",
        f"set -x",
        f"for core in $(ls /cores); do",
        f"    binary_path_pid=$(echo $core | sed 's/!/\\//g')",
        f"    binary_path=$(echo $binary_path_pid | sed 's/\\.[0-9]*$//')",
        f"    binary_pid=$(basename $binary_path_pid)",
        f"    backtrace={backtrace_dir}/$binary_pid.backtrace",
        f"    sudo {gdb} $binary_path /cores/$core {gdb_args} | sudo tee $backtrace",
        f"done"])
    ssh(f"sudo tee /process_coredumps.sh <<'EOF' && sudo chmod +x /process_coredumps.sh\n{script}\nEOF")


def process_coredumps(ssh):
    ssh(f"sudo /process_coredumps.sh")
