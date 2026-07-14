import json
import os
import time

import yatest.common as common

from cloud.filestore.tools.testing.loadtest.protos.loadtest_pb2 import TTestGraph, ACTION_WRITE, ACTION_READ
from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from google.protobuf.text_format import MessageToString

BLOCK_SIZE = 4 * 1024
SHARD_SIZE = 1024 * 1024 * 1024


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
    load_test.Name = "diagnose-shards"
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


def __list_loadtest_files(client, fs_id, client_id):
    output = json.loads(client.ls(
        fs_id,
        "/",
        "--json",
        "--disable-multitablet-forwarding",
    ))
    prefix = client_id + ":"
    files = []
    for entry in output["content"]:
        name = entry.get("Name")
        if not name or not name.startswith(prefix):
            continue

        files.append({
            "name": name,
            "shard_id": entry.get("ShardFileSystemId"),
        })

    return files


def test_diagnose_shards_with_loadtest():
    client, client_nocheck, _ = __init_test()
    shards_count = 4
    blocks_count = shards_count * int(SHARD_SIZE / BLOCK_SIZE)
    loadtest_duration_seconds = 20
    poll_interval_seconds = 1
    poll_deadline_seconds = 20
    top_n_shards = 3
    loadtest_client_id = "diagnose-shards-loadtest"
    workload = None
    target_shards = None
    diagnose_response = None
    files_response = None
    seen_active_shards = set()

    try:
        client.create(
            "fs0",
            "test_cloud",
            "test_folder",
            BLOCK_SIZE,
            blocks_count)
        client.resize("fs0", blocks_count, shard_count=shards_count)

        topology = json.loads(client.execute_action(
            "getfilesystemtopology",
            {"FileSystemId": "fs0"}))
        assert len(topology["ShardFileSystemIds"]) == shards_count

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

            files_response = __list_loadtest_files(client, "fs0", loadtest_client_id)
            target_shards = {file["shard_id"] for file in files_response}

            diagnose_response = json.loads(client.diagnose_shards(
                "fs0",
                top=top_n_shards,
            ))
            assert len(diagnose_response["shards"]) == top_n_shards
            active_shards = {
                shard["shard_id"]
                for shard in diagnose_response["shards"]
                if shard["current_load"] > 0
            }
            seen_active_shards.update(active_shards)

            if target_shards and target_shards.issubset(active_shards):
                break

            if not workload.running:
                workload.wait(check_exit_code=False)
                break

        if workload is not None and workload.running:
            workload.wait(check_exit_code=False)

        assert target_shards is not None and target_shards.issubset(seen_active_shards), (
            "Target file shards are not active. files={}, active_shards={}, "
            "diagnose_response={}".format(
                files_response,
                seen_active_shards,
                diagnose_response))
    finally:
        if workload is not None and workload.running:
            workload.kill()
            workload.wait(check_exit_code=False)
        client_nocheck.destroy("fs0")
