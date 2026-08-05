import argparse
import logging
import os

import yatest.common as common

from library.python.testing.recipe import declare_recipe, set_env

from contrib.ydb.tests.library.harness.kikimr_runner import (
    get_unique_path_for_current_test,
    ensure_path_exists,
)

import cloud.blockstore.tests.python.lib.daemon as bs_daemon
from cloud.blockstore.tests.python.lib.config import (
    NbsConfigurator,
    generate_disk_agent_txt,
)
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

logger = logging.getLogger(__name__)

#
# The recipe brings up an isolated blockstore stack (its own ydb, nbsd,
# diskagentd) that exists only to serve the fastshard's TWriteLogRecord /
# TReadPages protocol via the disk-agent's journalled_device_tcp_server.
# The filestore side stays untouched -- its own kikimr recipe still owns
# NFS_SERVER_PORT etc.
#

YDB_PID_FILE = "blockstore_disk_agent_recipe.ydb_pid"
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
        pid = int(f.read())
    try:
        os.kill(pid, 9)
    except OSError:
        pass


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--device-count",
        type=int,
        default=3,
        help="how many devices the disk-agent should expose",
    )
    parser.add_argument(
        "--device-size",
        type=int,
        default=128 * 1024 * 1024,
        help="size of each device in bytes",
    )
    args = parser.parse_args(argv)

    #
    # 1. Isolated YDB + NBS.
    #

    ydb = bs_daemon.start_ydb()
    with open(YDB_PID_FILE, "w") as f:
        f.write(str(list(ydb.nodes.values())[0].pid))

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
            "Kind": "DEVICE_POOL_KIND_LOCAL",
            "AllocationUnit": args.device_size,
        }],
    })

    #
    # 2. Raw device files. One backing file with `device_count` slots.
    #

    data_root = get_unique_path_for_current_test(
        output_path=common.output_path(),
        sub_folder="disk_agent_data",
    )
    data_dir = os.path.join(data_root, "dev", "disk", "by-partlabel")
    ensure_path_exists(data_dir)

    dev_file = os.path.join(data_dir, "NVMEFS01")
    total_bytes = (
        DEVICE_HEADER
        + args.device_size * args.device_count
        + (args.device_count - 1) * DEVICE_PADDING
    )
    with open(dev_file, "wb") as f:
        os.truncate(f.fileno(), total_bytes)

    #
    # 3. Disk agent with journalled_device_tcp_server exposed.
    #

    import contrib.ydb.tests.library.common.yatest_common as _yatest_common
    tcp_port = _yatest_common.PortManager().get_port()

    da_cfg = NbsConfigurator(ydb, node_type="disk-agent")
    da_cfg.generate_default_nbs_configs()
    da_conf = generate_disk_agent_txt(
        agent_id="",
        storage_discovery_config={
            "PathConfigs": [{
                "BlockSize": DEVICE_BLOCK_SIZE,
                "PathRegExp": f"{data_dir}/NVMEFS([0-9]+)",
                "PoolConfigs": [{
                    "PoolName": STORAGE_POOL_NAME,
                    "Layout": {
                        "DeviceSize": args.device_size,
                        "DevicePadding": DEVICE_PADDING,
                        "HeaderSize": DEVICE_HEADER,
                    },
                }],
            }],
        },
    )
    da_conf.JournalledDeviceTcpServerListenAddress = f"localhost:{tcp_port}"
    da_cfg.files["disk-agent"] = da_conf

    disk_agent = bs_daemon.start_disk_agent(da_cfg)
    disk_agent.wait_for_registration()
    with open(DA_PID_FILE, "w") as f:
        f.write(str(disk_agent.pid))

    #
    # 4. Register the agent's host with NBS and pull the device UUIDs
    # straight out of the disk-registry state. We deliberately do NOT
    # allocate a volume: the naive_mirrored fastshard calls
    # AcquireDevices via the TCP protocol on its own, and any NBS-side
    # lease from a volume would race with that acquire.
    #

    agent_id = bs_daemon.get_fqdn()
    nbs_client.add_host(agent_id)
    nbs_client.wait_for_devices_to_be_cleared()

    state = nbs_client.backup_disk_registry_state()
    agents = [a for a in state.get("Agents", []) if a.get("AgentId") == agent_id]
    assert len(agents) == 1, (
        f"expected exactly one agent named {agent_id!r}, got {len(agents)}: "
        f"{[a.get('AgentId') for a in state.get('Agents', [])]}"
    )
    device_uuids = [d["DeviceUUID"] for d in agents[0].get("Devices", [])]
    assert len(device_uuids) == args.device_count, (
        f"expected {args.device_count} devices, got {len(device_uuids)}: "
        f"{device_uuids}"
    )

    #
    # 5. Publish the endpoint so the fastshard test can consume it.
    #

    set_env("FASTSHARD_DA_HOST", "localhost")
    set_env("FASTSHARD_DA_PORT", str(tcp_port))
    set_env("FASTSHARD_DA_DEVICE_UUIDS", ",".join(device_uuids))
    set_env("FASTSHARD_DA_DEVICE_SIZE", str(args.device_size))

    logger.info(
        "blockstore-disk-agent recipe up: tcp=localhost:%d devices=%s",
        tcp_port,
        device_uuids,
    )


def stop(argv):
    _shutdown(DA_PID_FILE)
    _shutdown(NBS_PID_FILE)
    _shutdown(YDB_PID_FILE)


if __name__ == "__main__":
    declare_recipe(start, stop)
