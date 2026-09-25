"""End-to-end Blockstore dynamic configuration scenarios.

The suite starts real YDB and NBS processes and covers startup, update rejection,
removal, and startup with DynamicYamlConfigurationEnabled=false.
"""

import copy
import re
import time

from pathlib import Path

import pytest
import requests
import yaml

import yatest.common as yatest_common
from yatest.common.network import PortManager

from google.protobuf.text_format import MessageToString

from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig
from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.daemon import Nbs, start_nbs, start_ydb

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from contrib.ydb.public.api.protos.draft import ydb_dynamic_config_pb2
from contrib.ydb.core.protos.config_pb2 import TAppConfig
from contrib.ydb.core.protos.console_config_pb2 import TConfigItem
from contrib.ydb.tests.library.clients.kikimr_dynconfig_client import (
    dynconfig_client_factory,
)


DATABASE = "/Root/nbs"


# Build main YAML from the running YDB settings for Console validation.
def make_main_config(ydb):
    config = copy.deepcopy(ydb.config.yaml_config)
    config["feature_flags"]["database_yaml_config_allowed"] = True
    return yaml.safe_dump({
        "metadata": {"kind": "MainConfig", "cluster": "", "version": 0},
        "config": config,
    })


def make_database_config(version, config_yaml):
    if config_yaml:
        body = "\n".join(
            f"    {line}" if line else ""
            for line in config_yaml.splitlines()
        )
        config = f"private_database_config:\n{body}"
    else:
        config = "{}"

    return f"""---
metadata:
  kind: DatabaseConfig
  database: "{DATABASE}"
  version: {version}
config:
  {config}
"""


def replace_config(ydb, config, allow_absent_database=False):
    node = next(iter(ydb.nodes.values()))
    client = dynconfig_client_factory(node.host, node.port)
    try:
        request = ydb_dynamic_config_pb2.ReplaceConfigRequest(
            config=config,
            allow_absent_database=allow_absent_database,
        )
        response = client.invoke(request, "ReplaceConfig")
        assert response.operation.status == StatusIds.SUCCESS
    finally:
        client.close()


def replace_database_config(ydb, version, config_yaml):
    replace_config(
        ydb,
        make_database_config(version, config_yaml),
        allow_absent_database=True,
    )


def make_nbs_config(ydb, enabled):
    config = NbsConfigurator(ydb)
    config.generate_default_nbs_configs()
    config.files["server"].ServerConfig.DynamicYamlConfigurationEnabled = enabled
    config.files["storage"].NodeType = "nbs"
    config.files["storage"].DisableLocalService = True
    return config


# Read the PrivateDatabaseConfig subscription from the dispatcher monitoring page.
def get_config_subscription(nbs):
    page = requests.get(
        f"http://localhost:{nbs.mon_port}/actors/configs_dispatcher",
        timeout=10,
    )
    page.raise_for_status()
    subscription = re.search(
        rf"- Kinds: (?:PrivateDatabaseConfigItem|{TConfigItem.PrivateDatabaseConfigItem})\n"
        r"(.*?)(?=\n- Kinds:|\nSubscribers:)",
        page.text,
        re.DOTALL,
    )
    return subscription.group(1) if subscription else None


# Wait for a new PrivateDatabaseConfig delivery and its expected acknowledgement state.
def wait_config_delivery(nbs, previous_updates=0, acknowledged=True, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        subscription = get_config_subscription(nbs)
        if subscription:
            updates = int(re.search(r"UpdatesSent: (\d+)", subscription).group(1))
            pending = "UpdateInProcess:" in subscription
            if updates > previous_updates and pending != acknowledged:
                return updates
        time.sleep(1)
    raise AssertionError(f"PrivateDatabaseConfig delivery did not complete: {subscription}")


# Wait until ConfigsManager reports the requested number of runtime rejections.
def wait_runtime_rejection(nbs, count=1, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        log = Path(nbs.stderr_file_name).read_text()
        if log.count("Keeping the last successfully applied configuration") >= count:
            return log
        time.sleep(1)
    raise AssertionError("ConfigsManager did not report the rejected configuration")


def start_dynamic_config_ydb():
    return start_ydb(
        extra_feature_flags=["database_yaml_config_allowed"],
    )


# Verify that both bootstraps initialize Local/Null without a YDB snapshot.
@pytest.mark.parametrize("binary", [
    "server/nbsd",
    "server_lightweight/nbsd-lightweight",
])
@pytest.mark.parametrize("service", ["local", "null"])
def test_startup_without_ydb(binary, service, tmp_path):
    # Exercise the local configuration path without starting YDB.
    with PortManager() as ports:
        server_port = ports.get_port()
        mon_port = ports.get_port()
        nbs = Nbs(
            mon_port=mon_port,
            server_port=server_port,
            commands=[[
                yatest_common.binary_path(f"cloud/blockstore/apps/{binary}"),
                "--service", service,
                "--server-port", str(server_port),
                "--mon-port", str(mon_port),
            ]],
            cwd=str(tmp_path),
        )
        try:
            nbs.start()

            # Reach the listener after all early configuration consumers ran.
            yatest_common.execute([
                yatest_common.binary_path(
                    "cloud/blockstore/apps/client/blockstore-client"),
                "ping",
                "--host", "localhost",
                "--port", str(server_port),
                "--timeout", "10",
            ], timeout=30)
        finally:
            nbs.stop()


# Verify that startup factories and actors use the same private configuration.
def test_startup_configures_disk_agent_backend_and_listener(tmp_path):
    # Provide a real file device while the local configuration disables its agent.
    device_path = tmp_path / "device.data"
    with device_path.open("wb") as device:
        device.truncate(4 * 1024 * 1024)
    device_id = "private-startup-device"

    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        config = make_nbs_config(ydb, True)
        config.files["disk-agent"] = TDiskAgentConfig(Enabled=False)

        # Enable the backend and select a listener port exclusively through YAML.
        with PortManager() as ports:
            private_port = ports.get_port()
            assert private_port != config.server_port
            replace_config(ydb, make_main_config(ydb))
            replace_database_config(ydb, 0, yaml.safe_dump({
                "server": {"server_config": {"port": private_port}},
                "disk_agent": {
                    "enabled": True,
                    "backend": "DISK_AGENT_BACKEND_AIO",
                    "file_devices": [{
                        "path": str(device_path),
                        "block_size": 4096,
                        "device_id": device_id,
                        "serial_number": "private-startup-serial",
                        "device_model": "test-file",
                    }],
                },
            }))
            nbs = start_nbs(config)

            # Connect to the effective listener instead of the port in the file.
            yatest_common.execute([
                yatest_common.binary_path(
                    "cloud/blockstore/apps/client/blockstore-client"),
                "ping",
                "--host", "localhost",
                "--port", str(private_port),
                "--timeout", "10",
            ], timeout=30)

            # Check the initialized device rather than only its configuration.
            def device_is_online():
                page = requests.get(
                    f"http://localhost:{nbs.mon_port}/blockstore/disk_agent",
                    timeout=10,
                )
                page.raise_for_status()
                row = re.search(
                    rf"<tr>\s*<td>{device_id}</td>(.*?)</tr>",
                    page.text,
                    re.DOTALL,
                )
                return row is not None and ">online</font>" in row.group(1)

            yatest_common.wait_for(
                device_is_online,
                timeout=30,
                fail_message="Private YAML file device did not become online",
            )
    finally:
        if nbs:
            nbs.stop()
        ydb.stop()


# Verify startup delivery, replacement, rejection, and removal with real nodes.
def test_dynamic_blockstore_config_lifecycle():
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # A database config is resolved together with the main YAML config.
        # Console therefore requires a non-empty main document first.
        replace_config(ydb, make_main_config(ydb))
        replace_database_config(
            ydb,
            0,
            "storage_service:\n  write_blob_threshold: 200",
        )

        # Distinguish the PrivateDatabaseConfig startup seed from the static configuration.
        config = make_nbs_config(ydb, True)
        config.files["storage"].WriteBlobThreshold = 100
        nbs = start_nbs(config)

        # Observe the applied seed in the configuration retained by a startup consumer.
        page = requests.get(f"http://localhost:{nbs.mon_port}/blockstore/service", timeout=10)
        page.raise_for_status()
        assert re.search(r"<td>WriteBlobThreshold</td>\s*<td>200</td>", page.text)
        log = Path(nbs.stderr_file_name).read_text()
        assert re.search(r"BLOCKSTORE_SERVER.*Received CMS configuration for YAML mode", log)
        assert re.search(r"BLOCKSTORE_SERVER.*Applied startup PrivateDatabaseConfig", log)
        updates = wait_config_delivery(nbs)

        replace_database_config(
            ydb,
            1,
            "storage_service:\n  write_blob_threshold: 300",
        )
        updates = wait_config_delivery(nbs, updates)

        replace_database_config(
            ydb,
            2,
            "storage_service: invalid",
        )
        log = wait_runtime_rejection(nbs)
        updates = wait_config_delivery(nbs, updates, acknowledged=False)
        # TODO: restore after fixing startup AppCriticalEvents regisgration
        # parse_errors = nbs.counters.find({
        #     "component": "server",
        #     "sensor": "AppCriticalEvents/GetConfigsFromCmsYamlParseError",
        # })
        # assert parse_errors is not None and parse_errors["value"] >= 1
        assert "Keeping the last successfully applied configuration" in log
        assert "Fix or roll back the cluster configuration before restarting nodes" in log
        assert "will be lost after a restart" in log

        # Recover after an error without restarting the subscriber.
        replace_database_config(
            ydb,
            3,
            "storage_service:\n  write_blob_threshold: 400",
        )
        updates = wait_config_delivery(nbs, updates)

        # Remove an invalid source after another rejection.
        replace_database_config(ydb, 4, "storage_service: invalid")
        wait_runtime_rejection(nbs, count=2)
        updates = wait_config_delivery(nbs, updates, acknowledged=False)
        replace_database_config(ydb, 5, "")
        wait_config_delivery(nbs, updates)
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()


# Verify that startup filters static-only overrides while applying mutable settings.
def test_static_only_fields_are_ignored_at_startup():
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # Mix a mutable setting with overrides of node identity and dispatcher settings.
        replace_config(ydb, make_main_config(ydb))
        replace_database_config(
            ydb,
            0,
            "storage_service:\n"
            "  write_blob_threshold: 200\n"
            "  node_type: other\n"
            "  scheme_shard_dir: /Root/other\n"
            "  config_dispatcher_settings:\n"
            "    deny_list:\n"
            "      names: [PrivateDatabaseConfigItem]\n"
            "    additional_node_labels:\n"
            "      - key: private_label\n"
            "        value: dynamic",
        )
        config = make_nbs_config(ydb, True)
        config.files["storage"].WriteBlobThreshold = 100
        config.files["storage"].ConfigDispatcherSettings.AdditionalNodeLabels.add(
            Key="zone",
            Value="local",
        )

        # Publish the startup PrivateDatabaseConfig and complete its first runtime delivery.
        nbs = start_nbs(config)
        wait_config_delivery(nbs)

        # Keep static node identity while applying the mutable value to a startup consumer.
        page = requests.get(f"http://localhost:{nbs.mon_port}/blockstore/service", timeout=10)
        page.raise_for_status()
        assert re.search(r"<td>NodeType</td>\s*<td>nbs</td>", page.text)
        assert re.search(rf"<td>SchemeShardDir</td>\s*<td>{DATABASE}</td>", page.text)
        assert re.search(r"<td>WriteBlobThreshold</td>\s*<td>200</td>", page.text)

        # Keep the original labels despite the PrivateDatabaseConfig NodeType and label additions.
        page = requests.get(
            f"http://localhost:{nbs.mon_port}/actors/configs_dispatcher",
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        page.raise_for_status()
        labels = {label["name"]: label["value"] for label in page.json()["labels"]}
        assert labels == {"tenant": DATABASE, "node_type": "nbs", "zone": "local"}
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()


# Verify that absent and empty PrivateDatabaseConfig preserve static startup values.
@pytest.mark.parametrize("config_yaml", ["", "{}"], ids=["absent", "empty"])
def test_startup_without_overrides(config_yaml):
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # Supply either an absent or a valid empty PrivateDatabaseConfig.
        replace_config(ydb, make_main_config(ydb))
        replace_database_config(ydb, 0, config_yaml)
        config = make_nbs_config(ydb, True)
        config.files["storage"].WriteBlobThreshold = 100

        # Preserve the static value without reporting a rejected startup source.
        nbs = start_nbs(config)
        page = requests.get(f"http://localhost:{nbs.mon_port}/blockstore/service", timeout=10)
        page.raise_for_status()
        assert re.search(r"<td>WriteBlobThreshold</td>\s*<td>100</td>", page.text)
        log = Path(nbs.stderr_file_name).read_text()
        assert "Starting without PrivateDatabaseConfig" not in log
        assert "No startup PrivateDatabaseConfig applied; using configuration after CMS" in log
        updates = wait_config_delivery(nbs)

        # Accept a later PrivateDatabaseConfig through the already registered subscriber.
        replace_database_config(
            ydb,
            1,
            "storage_service:\n  write_blob_threshold: 300",
        )
        wait_config_delivery(nbs, updates)
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()


# Verify that invalid PrivateDatabaseConfig YAML preserves startup settings and allows recovery.
def test_invalid_config_is_skipped_at_startup():
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # Distinguish the accepted CMS value from the static configuration.
        main_config = yaml.safe_load(make_main_config(ydb))
        main_config["config"]["blockstore_config"] = {"volume_preemption_type": 2}
        replace_config(ydb, yaml.safe_dump(main_config))
        replace_database_config(
            ydb,
            0,
            "storage_service:\n  write_blob_threshold: 200\n"
            "server: private-secret-value",
        )
        config = make_nbs_config(ydb, True)
        config.files["storage"].WriteBlobThreshold = 100
        config.files["storage"].VolumePreemptionType = 1

        # Start without any PrivateDatabaseConfig values, retaining the supported CMS setting.
        nbs = start_nbs(config)
        initial_pid = nbs.pid
        page = requests.get(f"http://localhost:{nbs.mon_port}/blockstore/service", timeout=10)
        page.raise_for_status()
        assert re.search(r"<td>WriteBlobThreshold</td>\s*<td>100</td>", page.text)
        assert re.search(
            r"<td>VolumePreemptionType</td>\s*<td>(2|PREEMPTION_MOVE_LEAST_HEAVY)</td>",
            page.text,
        )
        log = Path(nbs.stderr_file_name).read_text()
        assert "CRITICAL_EVENT:AppCriticalEvents/GetConfigsFromCmsYamlParseError" in log
        assert "Starting without PrivateDatabaseConfig" in log
        assert "expected json map" in log

        # Keep the invalid runtime delivery rejected rather than accepting removal.
        wait_runtime_rejection(nbs)
        updates = wait_config_delivery(nbs, acknowledged=False)

        # Accept the corrected source in the same process.
        replace_database_config(
            ydb,
            1,
            "storage_service:\n  write_blob_threshold: 300",
        )
        wait_config_delivery(nbs, updates)
        assert nbs.pid == initial_pid
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()


# Verify the static mode and RDMA rebuild when CMS preserves or removes legacy fields.
@pytest.mark.parametrize("remove_rdma_config", [False, True], ids=["rdma_unchanged", "rdma_removed"])
def test_disabled_feature_does_not_start_configs_manager(remove_rdma_config):
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # Supply the opposite flag through the PROTO ServerAppConfig in CMS.
        config = make_nbs_config(ydb, False)
        config.files["storage"].ConfigsDispatcherServiceEnabled = True
        config.files["server"].ServerConfig.RdmaClientEnabled = True
        config.files["server"].ServerConfig.RdmaClientConfig.QueueSize = 128
        config.files["server"].ServerConfig.UseFakeRdmaClient = True
        cms_server = copy.deepcopy(config.files["server"])
        cms_server.ServerConfig.DynamicYamlConfigurationEnabled = True
        if remove_rdma_config:
            cms_server.ServerConfig.ClearField("RdmaClientEnabled")
            cms_server.ServerConfig.ClearField("RdmaClientConfig")
        cms_config = TAppConfig()
        named_config = cms_config.NamedConfigs.add()
        named_config.Name = "Cloud.NBS.ServerAppConfig"
        named_config.Config = MessageToString(cms_server).encode()
        ydb.client.add_config_item(cms_config)

        # Start with the static mode even after ServerConfig is replaced by CMS.
        nbs = start_nbs(config)
        assert get_config_subscription(nbs) is None

        # Build the static RDMA source before CMS and report only an actual change.
        log = Path(nbs.stderr_file_name).read_text()
        assert log.index("Static RDMA config initialized from legacy fields") < log.index("CMS configs initialized")
        updated = "RDMA config updated from legacy fields after applying CMS configs"
        assert log.count(updated) == int(remove_rdma_config)
        assert ("Fake RDMA client initialized" in log) == (not remove_rdma_config)
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()


# Verify that temporary startup retains CMS while ignoring private YAML and updates.
@pytest.mark.parametrize("invalid_private_config", [False, True], ids=["valid", "invalid"])
def test_temporary_server_skips_private_config(tmp_path, invalid_private_config):
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # Distinguish common CMS, legacy NamedConfigs, and private YAML sources.
        main_config = yaml.safe_load(make_main_config(ydb))
        main_config["config"]["blockstore_config"] = {"volume_preemption_type": 2}
        replace_config(ydb, yaml.safe_dump(main_config))
        cms_config = TAppConfig()
        named = cms_config.NamedConfigs.add()
        named.Name = "Cloud.NBS.StorageServiceConfig"
        named.Config = b"WriteBlobThreshold: 300"
        ydb.client.add_config_item(cms_config)
        private_config = "storage_service: invalid" if invalid_private_config else yaml.safe_dump({
            "storage_service": {
                "write_blob_threshold": 200,
                "remote_mount_only": False,
                "disable_manually_preempted_volumes_tracking": False,
            },
            "server": {"server_config": {"port": 0}},
        })
        replace_database_config(ydb, 0, private_config)

        # Use a local nameservice marker that must be replaced by the CMS config.
        config = make_nbs_config(ydb, True)
        config.files["storage"].WriteBlobThreshold = 100
        config.files["storage"].VolumePreemptionType = 1
        config.files["naming"] = copy.deepcopy(ydb.config.names_txt)
        config.files["naming"].ClusterUUID = "temporary-local-nameservice"
        config_path = tmp_path / "cfg"
        config_path.mkdir()
        config.install(str(config_path))
        nbs = Nbs(
            mon_port=config.mon_port,
            server_port=config.server_port,
            commands=[[
                yatest_common.binary_path("cloud/blockstore/apps/server/nbsd"),
                *config.params,
                "--temporary-server",
                "--server-port", str(config.server_port),
            ]],
            cwd=str(tmp_path),
        )
        nbs.start()

        # Preserve temporary restrictions and local NBS values without PROTO fallback.
        page = requests.get(f"http://localhost:{nbs.mon_port}/blockstore/service", timeout=10)
        page.raise_for_status()
        assert re.search(r"<td>WriteBlobThreshold</td>\s*<td>100</td>", page.text)
        assert re.search(r"<td>RemoteMountOnly</td>\s*<td>(1|true)</td>", page.text)
        assert re.search(
            r"<td>DisableManuallyPreemptedVolumesTracking</td>\s*<td>(1|true)</td>",
            page.text,
        )
        assert re.search(
            r"<td>VolumePreemptionType</td>\s*<td>(2|PREEMPTION_MOVE_LEAST_HEAVY)</td>",
            page.text,
        )
        yatest_common.execute([
            yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client"),
            "ping", "--host", "localhost", "--port", str(config.server_port),
            "--timeout", "10",
        ], timeout=30)

        # Check the actual startup AppConfig, independently of later CMS deliveries.
        page = requests.get(f"http://localhost:{nbs.mon_port}/actors/configs_dispatcher", timeout=10)
        page.raise_for_status()
        startup_config = page.text.split("id='effective-startup-config'", 1)[1]
        startup_config = startup_config.split("data-target='#effective-dynamic-config'", 1)[0]
        assert "NameserviceConfig" in startup_config
        assert "temporary-local-nameservice" not in startup_config
        assert get_config_subscription(nbs) is None
        log = Path(nbs.stderr_file_name).read_text()
        assert "Received CMS configuration for YAML mode; PrivateDatabaseConfig: no" in log
        assert "GetConfigsFromCmsYamlParseError" not in log
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()
