import json
import os
import time
import yatest.common as common

from cloud.filestore.tools.testing.loadtest.protos.loadtest_pb2 import TTestGraph, ACTION_WRITE, ACTION_READ
from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from google.protobuf.text_format import MessageToString
from copy import deepcopy

BLOCK_SIZE = 4 * 1024
SHARD_SIZE = 1024 * 1024 * 1024
UNSTABLE_DIAGNOSE_FIELDS = (
    "current_load",
    "suffer",
    "used_blocks_count",
    "used_nodes_count",
)


def __init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())
    client_nocheck = FilestoreCliClient(
        binary_path,
        port,
        cwd=common.output_path(),
        check_exit_code=False)

    results_path = common.output_path() + "/results.txt"
    return client, client_nocheck, results_path


def __generate_loadtest_config(
    fs_id,
    client_id,
    test_duration_seconds,
):
    config = TTestGraph()
    config.Tests.add()

    load_test = config.Tests[0].LoadTest
    load_test.Name = "diagnose-filesystem"
    load_test.FileSystemId = fs_id
    load_test.ClientId = client_id
    # 1 outstanding filesystem operation should not create more than MinFileCount files
    load_test.IODepth = 1
    load_test.TestDuration = test_duration_seconds

    load_spec = load_test.DataLoadSpec
    load_spec.ReadBytes = 4 * 1024
    load_spec.WriteBytes = 4 * 1024
    load_spec.InitialFileSize = 64 * 1024
    load_spec.ValidationEnabled = False
    load_spec.MinFileCount = 2
    load_spec.MinFileSize = 4 * 1024
    load_spec.AppendPercentage = 100
    load_spec.Actions.add(Action=ACTION_WRITE, Rate=50)
    load_spec.Actions.add(Action=ACTION_READ, Rate=100)

    config_path = os.path.join(common.output_path(), "loadtest.txt")
    with open(config_path, "w") as config_file:
        config_file.write(MessageToString(config))

    return config_path


def __json_line(data):
    return json.dumps(data, indent=4, sort_keys=True).encode("utf-8") + b"\n"


def __get_loadtested_shards(client, fs_id, client_id):
    output = json.loads(client.ls(
        fs_id,
        "/",
        "--json",
        "--disable-multitablet-forwarding",
    ))
    prefix = client_id + ":"
    shard_ids = []
    for entry in output["content"]:
        name = entry.get("Name")
        if not name or not name.startswith(prefix):
            continue

        shard_id = entry.get("ShardFileSystemId")
        if shard_id:
            shard_ids.append(shard_id)

    return shard_ids


def __alias_shards(shard_ids):
    return {
        shard_id: f"shard_{i + 1}"
        for i, shard_id in enumerate(sorted(shard_ids))
    }


def __normalize_responses(diagnose_responses):
    if not diagnose_responses:
        return {}

    result = deepcopy(diagnose_responses[0])
    result_shards = {
        shard["shard_id"]: shard
        for shard in result["shards"]}

    for shard in result_shards.values():
        for field in UNSTABLE_DIAGNOSE_FIELDS:
            shard[field] = 0

    for response in diagnose_responses:
        input_shards = {
            shard["shard_id"]: shard
            for shard in response["shards"]
        }

        for shard_id, result_shard in result_shards.items():
            input_shard = input_shards[shard_id]
            for field in UNSTABLE_DIAGNOSE_FIELDS:
                result_shard[field] = (1 if input_shard[field] > 0 else result_shard[field])

    return result


def test_diagnose_filesystem_with_loadtest():
    client, client_nocheck, results_path = __init_test()
    shards_count = 4
    blocks_count = shards_count * int(SHARD_SIZE / BLOCK_SIZE)
    loadtest_duration_seconds = 20
    poll_interval_seconds = 1
    poll_deadline_seconds = 20
    top_n_shards = 3
    loadtest_client_id = "diagnose-filesystem-loadtest"
    workload = None
    target_shards = None
    diagnose_responses = []

    try:
        client.create(
            "fs0",
            "test_cloud",
            "test_folder",
            BLOCK_SIZE,
            blocks_count)
        client.resize("fs0", blocks_count, shard_count=shards_count)

        config_path = __generate_loadtest_config(
            "fs0",
            loadtest_client_id,
            loadtest_duration_seconds,
        )

        bin_path = common.binary_path(
            "cloud/filestore/tools/testing/loadtest/bin/filestore-loadtest"
        )
        workload = common.execute(
            [
                bin_path,
                "--port",
                os.getenv("NFS_SERVER_PORT"),
                "--tests-config",
                config_path,
            ],
            cwd=common.output_path(),
            wait=False,
            check_exit_code=False)

        deadline = time.time() + poll_deadline_seconds
        while time.time() < deadline:
            time.sleep(poll_interval_seconds)

            target_shards = set(__get_loadtested_shards(client, "fs0", loadtest_client_id))

            diagnose_response = json.loads(client.diagnose_filesystem(
                "fs0",
                top=top_n_shards,
            ))
            diagnose_responses.append(diagnose_response)
            active_shards = {
                shard["shard_id"]
                for shard in diagnose_response["shards"]
                if shard["current_load"] > 0
            }

            if target_shards and target_shards.issubset(active_shards):
                break

            if not workload.running:
                workload.wait(check_exit_code=False, timeout=loadtest_duration_seconds+10)
                break

        if workload is not None and workload.running:
            workload.wait(check_exit_code=False)

        normalized_response = __normalize_responses(diagnose_responses)
        response_shard_ids = {
            shard["shard_id"]
            for shard in normalized_response["shards"]
        }
        aliases = __alias_shards(response_shard_ids)
        aliased_targets = [
            aliases[shard_id]
            for shard_id in sorted(target_shards or set())
        ]
        for shard in normalized_response["shards"]:
            shard["shard_id"] = aliases[shard["shard_id"]]

        normalized_response["shards"].sort(
            key=lambda shard: shard["shard_id"]
        )
        out = __json_line(aliased_targets)
        out += __json_line(normalized_response)

    finally:
        if workload is not None and workload.running:
            workload.wait(check_exit_code=False, timeout=loadtest_duration_seconds+10)
        client_nocheck.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret
