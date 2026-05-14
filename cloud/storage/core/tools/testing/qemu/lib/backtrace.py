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
    ssh(f"sudo cp /cores/* {common.output_path()}")

    logging.info(f"looking for cores in {common.output_path()}")
    for core in os.listdir(common.output_path()):
        match = re.search(r"(.+)\.(\d+)\.core", core.replace("!", "/"))
        if not match:
            continue

        logging.info(f"extracting backtrace from {core}")
        binary_path = match.group(1)
        pid = match.group(2)
        binary = os.path.basename(binary_path)
        backtrace_path = os.path.join(common.output_path(), f"{binary}.{pid}.backtrace")
        core_path = os.path.join(common.output_path(), core)

        backtrace = get_backtrace(ssh, binary_path, core_path, backtrace_path)
