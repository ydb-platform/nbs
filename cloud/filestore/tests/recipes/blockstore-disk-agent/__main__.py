import argparse
import logging
import os

import yatest.common as common

from library.python.testing.recipe import declare_recipe

from contrib.ydb.tests.library.harness.kikimr_runner import (
    get_unique_path_for_current_test,
    ensure_path_exists,
)

import cloud.blockstore.tests.python.lib.daemon as bs_daemon
from cloud.blockstore.config.server_pb2 import TLocation
from cloud.blockstore.tests.python.lib.config import (
    NbsConfigurator,
    generate_disk_agent_txt,
)
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.filestore.tests.python.lib.common import shutdown

from cloud.storage.core.tests.ydb import Ydb

logger = logging.getLogger(__name__)

#
# The recipe adds a blockstore stack (nbsd, diskagentd) to the ydb of the ydb
# recipe. The filestore service uses it to create fast shard enabled filestores.

NBS_PID_FILE = "blockstore_disk_agent_recipe.nbs_pid"
DA_PID_FILE = "blockstore_disk_agent_recipe.disk_agent_pid"

DEVICE_BLOCK_SIZE = 4096
DEVICE_HEADER = 4096
DEVICE_PADDING = 4096
STORAGE_POOL_NAME = "fastshard"


def _shutdown(pid_file):
    if not os.path.exists(pid_file):
        return
    with open(pid_file) as f:
        pids = f.read().split()

    for pid in pids:
        shutdown(int(pid))


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--agent-count",
        type=int,
        default=3,  # Quorum setup uses 3 devices in a placement group
        help="how many disk-agents to start, each in its own rack",
    )
    parser.add_argument(
        "--device-count",
        type=int,
        default=1,
        help="how many devices each disk-agent should expose",
    )
    parser.add_argument(
        "--device-size",
        type=int,
        default=128 * 1024 * 1024,
        help="size of each device in bytes",
    )
    args = parser.parse_args(argv)

    #
    # 1. NBS in its own tenant.
    #

    ydb = Ydb.from_recipe()
    ydb.create_tenant("nbs")
    ydb.enable_custom_cms_configs()

    nbs_cfg = NbsConfigurator(ydb)
    nbs_cfg.generate_default_nbs_configs()
    nbs_cfg.files["storage"].DisableLocalService = 0
    nbs_cfg.files["storage"].NonReplicatedDontSuspendDevices = True
    nbs_cfg.files["storage"].NonReplicatedAgentMinTimeout = 600000
    nbs_cfg.files["storage"].NonReplicatedAgentMaxTimeout = 600000

    nbs = bs_daemon.start_nbs(nbs_cfg)
    with open(NBS_PID_FILE, "w") as f:
        f.write(str(nbs.pid))

    nbs_client = CreateTestClient(f"localhost:{nbs.port}")
    nbs_client.execute_DiskRegistrySetWritableState(State=True)
    nbs_client.update_disk_registry_config({
        "KnownDevicePools": [{
            "Name": STORAGE_POOL_NAME,
            "Kind": "DEVICE_POOL_KIND_GLOBAL",
            "AllocationUnit": args.device_size,
        }],
    })

    #
    # 2. Raw device files and backing file with `device_count` slots each.
    #

    data_root = get_unique_path_for_current_test(
        output_path=common.output_path(),
        sub_folder="disk_agent_data",
    )
    total_bytes = (
        DEVICE_HEADER
        + args.device_size * args.device_count
        + (args.device_count - 1) * DEVICE_PADDING
    )

    import contrib.ydb.tests.library.common.yatest_common as _yatest_common
    port_manager = _yatest_common.PortManager()

    for i in range(args.agent_count):
        agent_id = f"agent-{i}"

        data_dir = os.path.join(data_root, agent_id)
        ensure_path_exists(data_dir)
        with open(os.path.join(data_dir, "NVMEFS01"), "wb") as f:
            os.truncate(f.fileno(), total_bytes)

        da_cfg = NbsConfigurator(
            ydb,
            node_type="disk-agent",
            location=TLocation(Rack=f"rack-{i}"))
        da_cfg.generate_default_nbs_configs()
        da_conf = generate_disk_agent_txt(
            agent_id=agent_id,
            device_erase_method="DEVICE_ERASE_METHOD_NONE",
            storage_discovery_config={
                "PathConfigs": [{
                    "BlockSize": DEVICE_BLOCK_SIZE,
                    "PathRegExp": f"{data_dir}/NVMEFS([0-9]+)",
                    "PoolConfigs": [{
                        "PoolName": STORAGE_POOL_NAME,
                        "JournalConfig": {"Enabled": True},
                        "Layout": {
                            "DeviceSize": args.device_size,
                            "DevicePadding": DEVICE_PADDING,
                            "HeaderSize": DEVICE_HEADER,
                        },
                    }],
                }],
            },
        )

        # The disk agent reports its devices at the fqdn.
        da_conf.JournalledDeviceTcpServerListenAddress = \
            f"{bs_daemon.get_fqdn()}:{port_manager.get_port()}"
        da_cfg.files["disk-agent"] = da_conf

        disk_agent = bs_daemon.start_disk_agent(da_cfg, name=agent_id)
        with open(DA_PID_FILE, "a") as f:
            f.write(f"{disk_agent.pid}\n")
        disk_agent.wait_for_registration()

        nbs_client.add_host(agent_id)

    nbs_client.wait_for_devices_to_be_cleared()


def stop(argv):
    _shutdown(DA_PID_FILE)
    _shutdown(NBS_PID_FILE)


if __name__ == "__main__":
    declare_recipe(start, stop)
