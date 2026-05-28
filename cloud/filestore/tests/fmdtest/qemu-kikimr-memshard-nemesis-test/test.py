import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.fastshard import configure_fastshard
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)


def do_test(test_name, aux_params):
    fast_shard_config = {
        "MemConfig": {
            "CreateNodeUponAccess": True,
        }
    }

    configure_fastshard(
        shard_count=20,
        file_shard_count=10,
        fast_shard_config=fast_shard_config)

    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    fmdtest_bin = common.binary_path(
        "cloud/filestore/tools/testing/fmdtest/bin/fmdtest")

    working_dir = os.path.join(mount_dir, test_name + "_wd")
    report_file = "report.json"

    ssh(f"{fmdtest_bin} --test-dir {working_dir} --report-path {report_file}"
        f" --skip-file-validation --create-file-attempt-count 10 {aux_params}")

    ret = ssh(f"sudo cat {report_file}")
    report = json.loads(ret.stdout.decode("utf8"))
    for k, v in report.items():
        report[k] = v > 0

    results_path = f"{common.output_path()}/{test_name}_results.txt"
    with open(results_path, 'w') as results:
        results.write(json.dumps(report, indent=4))

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_create_unlink_steal():
    return do_test(
        "create_unlink_steal",
        "--duration 60s --stealer-threads 1")
