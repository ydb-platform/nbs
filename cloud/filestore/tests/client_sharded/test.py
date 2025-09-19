import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient

BLOCK_SIZE = 4 * 1024
SHARD_SIZE = 1024 * 1024 * 1024


def __init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())

    results_path = common.output_path() + "/results.txt"
    return client, results_path


def __process_stat(node):
    def d(k):
        if k in node:
            del node[k]

    d("ATime")
    d("MTime")
    d("CTime")
    d("ShardNodeName")

    return node


def __exec_ls(client, *args):
    output = str(client.ls(*args, "--json"), 'utf-8')
    nodes: list = json.loads(output)['content']

    for node in nodes:
        __process_stat(node)

    return json.dumps(nodes, indent=4).encode('utf-8')


def test_shard_autoaddition():
    client, results_path = __init_test()
    out = client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE))

    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write("some data")

    client.write("fs0", "/xxx1", "--data", data_file)
    client.write("fs0", "/xxx2", "--data", data_file)
    out += __exec_ls(client, "fs0", "/")
    out += __exec_ls(client, "fs0", "/", "--disable-multitablet-forwarding")

    client.create_session("fs0", "session0", "client0")
    out += client.execute_action("describesessions", {"FileSystemId": "fs0"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s1"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s2"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s3"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s4"})
    client.destroy_session("fs0", "session0", "client0")

    client.create_session("fs0", "session0", "client0")
    out += client.resize("fs0", 3 * int(SHARD_SIZE / BLOCK_SIZE))
    # session should become orphaned after resize
    out += client.execute_action("describesessions", {"FileSystemId": "fs0"})
    client.destroy_session("fs0", "session0", "client0")

    client.write("fs0", "/xxx3", "--data", data_file)
    client.write("fs0", "/xxx4", "--data", data_file)
    client.write("fs0", "/xxx5", "--data", data_file)
    client.write("fs0", "/xxx6", "--data", data_file)
    client.write("fs0", "/xxx7", "--data", data_file)
    out += __exec_ls(client, "fs0", "/")
    out += __exec_ls(client, "fs0", "/", "--disable-multitablet-forwarding")

    client.create_session("fs0", "session0", "client0")
    out += client.execute_action("describesessions", {"FileSystemId": "fs0"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s1"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s2"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s3"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s4"})
    client.destroy_session("fs0", "session0", "client0")

    out += client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_explicit_shard_count_addition():
    client, results_path = __init_test()
    out = client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE))

    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write("some data")

    client.create_session("fs0", "session0", "client0")
    out = client.execute_action("describesessions", {"FileSystemId": "fs0"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s1"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s2"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s3"})
    client.destroy_session("fs0", "session0", "client0")

    out += client.resize("fs0", int(SHARD_SIZE / BLOCK_SIZE), shard_count=3)

    client.create_session("fs0", "session0", "client0")
    out += client.execute_action("describesessions", {"FileSystemId": "fs0"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s1"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s2"})
    out += client.execute_action("describesessions", {"FileSystemId": "fs0_s3"})
    client.destroy_session("fs0", "session0", "client0")

    out += client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_turn_on_strict():
    client, results_path = __init_test()
    out = client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE))

    out = client.create_session("fs0", "session0", "client0")
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0"})
    out += client.resize(
        "fs0", int(SHARD_SIZE / BLOCK_SIZE), shard_count=3, force=False,
        turn_on_strict=True)
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0"})
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0_s1"})
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0_s2"})
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0_s3"})
    out += client.destroy_session("fs0", "session0", "client0")

    out += client.destroy("fs0")
    out += client.destroy("fs0_s1")
    out += client.destroy("fs0_s2")
    out += client.destroy("fs0_s3")

    print(str(out, 'utf-8'))

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret
