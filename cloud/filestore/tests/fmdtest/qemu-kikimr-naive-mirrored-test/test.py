import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient

from cloud.filestore.tests.python.lib.fastshard import (
    configure_fastshard,
    fetch_layout,
)
from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)


def _build_persistent_config():
    #
    # Values published by the blockstore-disk-agent recipe.
    #

    host = os.environ["FASTSHARD_DA_HOST"]
    port = int(os.environ["FASTSHARD_DA_PORT"])
    uuids = os.environ["FASTSHARD_DA_DEVICE_UUIDS"].split(",")
    device_size = int(os.environ["FASTSHARD_DA_DEVICE_SIZE"])

    return {
        "PersistentConfig": {
            "StorageGroups": [{
                "Type": "E_SG_MIRROR",
                "Devices": [
                    {
                        "Host": host,
                        "Port": port,
                        "DeviceId": uuid,
                    }
                    for uuid in uuids
                ],
            }],
            "NodesPerGroup": 100000,
            #
            # ExpectedGroupCapacity is the projected user-data byte
            # count per storage group; the shard uses it to size its
            # per-cluster maps. Metadata footprint itself is small, but
            # still non-zero. 10% is probably a bit excessive but let's keep it
            # this way for this test to be on the safe side.
            #
            "ExpectedGroupCapacity": int(device_size * 0.9),
        },
    }


def do_test(test_name, aux_params):
    fast_shard_config = _build_persistent_config()

    #
    # file_shard_count=1: every fast shard would otherwise get the same
    # PersistentConfig (configure_fastshard fans one template out to all
    # shards), which means all shards would map the same page ranges on
    # the same physical devices and clobber each other. Memshard gets
    # away with fanning-out because each shard builds its own separate
    # in-memory state; naive_mirrored talks to real disks and cannot.
    # Provisioning one storage group per shard would need N * devices,
    # which is a bit excessive for this test.
    #

    file_shard_ids = configure_fastshard(
        shard_count=2,
        file_shard_count=1,
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

    for shard_id in file_shard_ids:
        shard = json.loads(client.describe(shard_id))
        # MainTabletId for a shard is actually its own IndexTabletId
        tablet_id = shard["FileStore"]["MainTabletId"]
        layout = fetch_layout(tablet_id)

        #
        # Masking unstable fields.
        #

        for sg in layout["storageGroups"]:
            for d in sg["devices"]:
                d["port"] = d.get("port", 0) != 0

        result = json.dumps(layout, indent=4)

        with open(results_path, 'a') as results:
            results.write('{}\n'.format(result))

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_create_unlink_steal():
    return do_test(
        "create_unlink_steal",
        "--duration 60s --stealer-threads 1")
