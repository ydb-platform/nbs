import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)


def do_test(test_name, aux_params, storage_config_patch=None):
    if storage_config_patch is not None:
        port = os.getenv("NFS_SERVER_PORT")
        binary_path = common.binary_path(
            "cloud/filestore/apps/client/filestore-client")
        client = FilestoreCliClient(
            binary_path,
            port,
            cwd=common.output_path())

        filesystem = os.getenv("NFS_FILESYSTEM")
        client.change_storage_service_config(filesystem, storage_config_patch)

    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    fmdtest_bin = common.binary_path(
        "cloud/filestore/tools/testing/fmdtest/bin/fmdtest")

    working_dir = os.path.join(mount_dir, test_name + "_wd")
    report_file = "report.json"

    ssh(f"{fmdtest_bin} --test-dir {working_dir} --report-path {report_file}"
        f" {aux_params}")

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


def test_create_list():
    return do_test(
        "create_list",
        "--duration 20s --unlink-percentage 0"
        " --stealer-threads 0 --lister-threads 1")


def test_create_unlink_steal_list_nodes_internal():
    return do_test(
        "create_unlink_steal_list_nodes_internal",
        "--duration 60s --stealer-threads 1",
        {"UseListNodesInternal": True})
