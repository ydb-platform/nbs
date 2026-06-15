import os
import logging

import yatest.common

from cloud.storage.core.tools.testing.qemu.lib.common import env_with_guest_index

logger = logging.getLogger(__name__)


class GuestClient:
    def __init__(self, local_ip, ssh_user, ssh_port, ssh_key):
        self.local_ip = local_ip
        self.ssh_user = ssh_user
        self.ssh_port = ssh_port
        self.ssh_key = ssh_key

    def get_local_ip(self):
        return self.local_ip

    def execute(self, command, *args, **kwargs):
        cmd = [
            "ssh",
            "-n",
            "-F",
            os.devnull,
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=" + os.devnull,
            "-o",
            "ConnectTimeout=10",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "ServerAliveInterval=10",
            "-o",
            "ServerAliveCountMax=10",
            "-i",
            self.ssh_key,
            "-l",
            self.ssh_user,
            "-p",
            str(self.ssh_port),
            "127.0.0.1",
        ]

        if isinstance(command, str):
            cmd.append(command)
        else:
            cmd.extend(command)

        logger.debug(">> %s", " ".join(cmd))
        return yatest.common.execute(cmd, *args, **kwargs)

    def get_rdma_test_util_path(self):
        return yatest.common.build_path(
            "cloud/blockstore/tools/testing/rdma-test/rdma-test"
        )


def setup_rdma():
    ssh_key = os.getenv("QEMU_SSH_KEY")

    clients = []
    for index, local_ip in enumerate(["192.168.1.1", "192.168.1.2"]):
        port = int(os.getenv(env_with_guest_index("QEMU_FORWARDING_PORT", index)))
        clients.append(
            GuestClient(
                local_ip=local_ip, ssh_user="qemu", ssh_port=port, ssh_key=ssh_key
            )
        )

        setup_cmds = [
            f"netplan set ethernets.ens5.addresses=[{local_ip}/24]",
            "netplan apply",
            "rdma link add rxe0 type rxe netdev ens5",
            "ln -s /usr/lib/x86_64-linux-gnu/libibverbs.so.1 /usr/lib/libibverbs.so",
            "ln -s /usr/lib/x86_64-linux-gnu/librdmacm.so.1 /usr/lib/librdmacm.so",
        ]

        for cmd in setup_cmds:
            clients[-1].execute("sudo " + cmd)

    return clients
