import os

import yatest.common as common

from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)


def do_test(test_name, aux_params):
    port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", 0)))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")

    ssh = SshToGuest(user="qemu", port=port, key=ssh_key)

    dirtree_bin = common.binary_path(
        "cloud/filestore/tools/testing/dirtree/bin/dirtree")

    working_dir = os.path.join(mount_dir, "wd")

    ret = ssh(f"{dirtree_bin} --test-dir {working_dir} --seed 111 {aux_params}")
    results_path = f"{common.output_path()}/{test_name}_results.txt"
    with open(results_path, 'w') as results:
        results.write(ret.stdout.decode("utf8"))

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_default():
    return do_test("default", "")
