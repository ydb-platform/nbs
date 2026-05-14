import logging
import os
import re

import yatest.common as common


def setup_coredumps(ssh):
    ssh("sudo mkdir /cores")
    ssh("sudo sysctl -w 'kernel.core_pattern=/cores/%E.%p.core'")
    ssh("echo '* soft core unlimited' | sudo tee -a /etc/security/limits.conf")
    ssh("echo '* hard core unlimited' | sudo tee -a /etc/security/limits.conf")


def get_backtrace(ssh, binary, core, backtrace):
    gdb = common.runtime.gdb_path()
    gdb_args = "--batch -iex 'set print thread-events off' -iex 'set auto-load safe-path /' -ex bt"
    ssh(f"sudo {gdb} {binary} {core} {gdb_args} | sudo tee {backtrace}")


def process_coredumps(ssh):
    core_dir = os.path.join(common.output_path(), "cores")
    backtrace_dir = os.path.join(common.output_path(), "backtraces")

    ssh(f"sudo cp -r /cores {core_dir}")
    ssh(f"sudo mkdir {backtrace_dir}")

    logging.info(f"looking for cores in {core_dir}")
    for core in os.listdir(core_dir):
        match = re.search(r"(.+)\.(\d+)\.core", core.replace("!", "/"))
        if not match:
            continue

        logging.info(f"extracting backtrace from {core}")
        binary_path = match.group(1)
        pid = match.group(2)
        binary = os.path.basename(binary_path)
        backtrace_path = os.path.join(backtrace_dir, f"{binary}.{pid}.backtrace")
        core_path = os.path.join(core_dir, core)

        get_backtrace(ssh, binary_path, core_path, backtrace_path)
