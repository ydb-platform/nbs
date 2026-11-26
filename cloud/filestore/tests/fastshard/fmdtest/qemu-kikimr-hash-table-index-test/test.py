import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient

from cloud.filestore.tests.python.lib.fastshard import fetch_layout
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)


def do_test(test_name, aux_params):
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
        results.write("\n")

    #
    # Dumping layout.
    #

    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(
        binary_path,
        port,
        cwd=common.output_path())

    request = {"FileSystemId": os.getenv("NFS_FILESYSTEM")}
    response = client.execute_action("getfilesystemtopology", request)
    topology = json.loads(response)

    for shard in topology["ShardTopology"]["Shards"]:
        if shard["Type"] != "E_FASTSHARD":
            continue

        layout = fetch_layout(shard["TabletId"])

        #
        # Masking unstable fields.
        #

        for sg in layout["storageGroups"]:
            for d in sg["devices"]:
                d["host"] = len(d["host"]) > 0
                d["port"] = d.get("port", 0) != 0
                d["deviceId"] = len(d["deviceId"]) > 0

        result = json.dumps(layout, indent=4)

        with open(results_path, 'a') as results:
            results.write('{}\n'.format(result))

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_create_unlink_steal():
    return do_test(
        "create_unlink_steal",
        "--duration 60s --stealer-threads 1")
