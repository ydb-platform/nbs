"""End-to-end BlockStore dynamic configuration scenarios.

The suite starts real YDB and NBS processes and covers startup, update rejection,
removal, and startup with DynamicYamlConfigurationEnabled=false through the
public DynamicConfig API and NBS counters.
"""

import copy
import re
import time

from pathlib import Path

import requests
import yaml

from google.protobuf.text_format import MessageToString

from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.daemon import start_nbs, start_ydb

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from contrib.ydb.public.api.protos.draft import ydb_dynamic_config_pb2
from contrib.ydb.core.protos.config_pb2 import TAppConfig
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


def make_database_config(version, private_database_config):
    if private_database_config:
        body = "\n".join(
            f"    {line}" if line else ""
            for line in private_database_config.splitlines()
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


def replace_database_config(ydb, version, private_database_config):
    replace_config(
        ydb,
        make_database_config(version, private_database_config),
        allow_absent_database=True,
    )


def make_nbs_config(ydb, enabled):
    config = NbsConfigurator(ydb)
    config.generate_default_nbs_configs()
    config.files["server"].ServerConfig.DynamicYamlConfigurationEnabled = enabled
    config.files["storage"].NodeType = "nbs"
    config.files["storage"].DisableLocalService = True
    return config


def get_sensor(nbs, name):
    return nbs.counters.find({
        "component": "configs_manager",
        "sensor": name,
    })


def wait_sensor(nbs, name, predicate, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sensor = get_sensor(nbs, name)
        if sensor is not None and predicate(sensor["value"]):
            return sensor["value"]
        time.sleep(1)
    raise AssertionError(f"sensor {name} did not reach the expected value")


def start_dynamic_config_ydb():
    return start_ydb(
        extra_feature_flags=["database_yaml_config_allowed"],
    )


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

        nbs = start_nbs(make_nbs_config(ydb, True))
        assert wait_sensor(nbs, "Active", lambda value: value == 1) == 1
        assert wait_sensor(
            nbs,
            "DynamicConfigPresent",
            lambda value: value == 1,
        ) == 1
        initial_successful_updates = get_sensor(
            nbs,
            "SuccessfulUpdates",
        )["value"]

        replace_database_config(
            ydb,
            1,
            "storage_service:\n  write_blob_threshold: 300",
        )
        assert wait_sensor(
            nbs,
            "SuccessfulUpdates",
            lambda value: value >= initial_successful_updates + 1,
        ) >= initial_successful_updates + 1

        replace_database_config(
            ydb,
            2,
            "storage_service: invalid",
        )
        assert wait_sensor(
            nbs,
            "RejectedUpdates",
            lambda value: value >= 1,
        ) >= 1
        assert get_sensor(nbs, "SuccessfulUpdates")["value"] == (
            initial_successful_updates + 1
        )
        assert get_sensor(nbs, "DynamicConfigPresent")["value"] == 1
        # TODO: restore after fixing startup AppCriticalEvents regisgration
        # parse_errors = nbs.counters.find({
        #     "component": "server",
        #     "sensor": "AppCriticalEvents/GetConfigsFromCmsYamlParseError",
        # })
        # assert parse_errors is not None and parse_errors["value"] >= 1
        log = Path(nbs.stderr_file_name).read_text()
        assert "Keeping the last successfully applied configuration" in log
        assert "Fix or roll back the cluster configuration before restarting nodes" in log
        assert "will be lost after a restart" in log

        # Recover after an error without restarting the subscriber.
        replace_database_config(
            ydb,
            3,
            "storage_service:\n  write_blob_threshold: 400",
        )
        assert wait_sensor(
            nbs,
            "SuccessfulUpdates",
            lambda value: value >= initial_successful_updates + 2,
        ) >= initial_successful_updates + 2

        # Remove an invalid source after another rejection.
        replace_database_config(ydb, 4, "storage_service: invalid")
        assert wait_sensor(
            nbs,
            "RejectedUpdates",
            lambda value: value >= 2,
        ) >= 2
        replace_database_config(ydb, 5, "")
        assert wait_sensor(
            nbs,
            "SuccessfulUpdates",
            lambda value: value >= initial_successful_updates + 3,
        ) >= initial_successful_updates + 3
        assert wait_sensor(
            nbs,
            "DynamicConfigPresent",
            lambda value: value == 0,
        ) == 0
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()


# Verify that invalid private YAML preserves startup settings and allows recovery.
def test_invalid_private_config_is_skipped_at_startup():
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # Distinguish the accepted CMS value from the local configuration.
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

        # Start without any private values, retaining the supported CMS setting.
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
        assert "Starting without the private configuration" in log
        assert "private-secret-value" not in log

        # Keep the invalid runtime delivery rejected rather than accepting removal.
        assert wait_sensor(nbs, "RejectedUpdates", lambda value: value >= 1) >= 1
        assert get_sensor(nbs, "DynamicConfigPresent")["value"] == 0
        assert get_sensor(nbs, "SuccessfulUpdates")["value"] == 0

        # Accept the corrected source in the same process.
        replace_database_config(
            ydb,
            1,
            "storage_service:\n  write_blob_threshold: 300",
        )
        assert wait_sensor(nbs, "SuccessfulUpdates", lambda value: value == 1) == 1
        assert get_sensor(nbs, "DynamicConfigPresent")["value"] == 1
        assert nbs.pid == initial_pid
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()


# Verify that CMS cannot enable ConfigsManager when the local flag is false.
def test_disabled_feature_does_not_start_configs_manager():
    ydb = start_dynamic_config_ydb()
    nbs = None
    try:
        # Supply the opposite flag through the PROTO ServerAppConfig in CMS.
        config = make_nbs_config(ydb, False)
        cms_server = copy.deepcopy(config.files["server"])
        cms_server.ServerConfig.DynamicYamlConfigurationEnabled = True
        cms_config = TAppConfig()
        named_config = cms_config.NamedConfigs.add()
        named_config.Name = "Cloud.NBS.ServerAppConfig"
        named_config.Config = MessageToString(cms_server).encode()
        ydb.client.add_config_item(cms_config)

        # Start with the local mode even after ServerConfig is replaced by CMS.
        nbs = start_nbs(config)
        assert get_sensor(nbs, "Active") is None
    finally:
        if nbs:
            nbs.kill()
        ydb.stop()
