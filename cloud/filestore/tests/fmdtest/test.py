import json
import os

import yatest.common as common

from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)


def test():
    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    fmdtest_bin = common.binary_path(
        "cloud/filestore/tools/testing/fmdtest/bin/fmdtest")

    working_dir = os.path.join(mount_dir, "wd")
    report_file = "report.json"

    ssh(f"{fmdtest_bin} --test-dir {working_dir} --report-path {report_file}"
        " --duration 60 --stealer-threads 1")

    ret = ssh(f"sudo cat {report_file}")
    report = json.loads(ret.stdout.decode("utf8"))
    for k, v in report.items():
        report[k] = v > 0

    results_path = common.output_path() + "/results.txt"
    with open(results_path, 'w') as results:
        results.write(json.dumps(report, indent=4))

    ret = common.canonical_file(results_path, local=True)
    return ret
