import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import NfsCliClient

BLOCK_SIZE = 4 * 1024
BLOCKS_COUNT = 1000


def __init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
    client = NfsCliClient(binary_path, port, cwd=common.output_path())

    results_path = common.output_path() + "/results.txt"
    return client, results_path


def __exec_ls(client, *args):
    output = str(client.ls(*args, "--json"), 'utf-8')
    nodes: list = json.loads(output)['content']

    for node in nodes:
        del node["ATime"]
        del node["MTime"]
        del node["CTime"]

    return json.dumps(nodes, indent=4).encode('utf-8')


def test_create_destroy():
    client, results_path = __init_test()

    out = client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    out += client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_create_mkdir_ls_destroy():
    client, results_path = __init_test()

    out = client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)

    client.mkdir("fs0", "/aaa")
    client.mkdir("fs0", "/bbb")

    out += __exec_ls(client, "fs0", "/")
    out += client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_create_mkdir_ls_write_destroy():
    client, results_path = __init_test()

    out = client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)

    client.mkdir("fs0", "/aaa")
    client.touch("fs0", "/first")
    out += __exec_ls(client, "fs0", "/")
    out += client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_list_filestores():
    client, results_path = __init_test()

    out = client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    out += client.create("fs1", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    out += client.create("fs2", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    out += client.create("fs3", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)

    out += ",".join(client.list_filestores()).encode()

    out += client.destroy("fs3")
    out += client.destroy("fs2")
    out += client.destroy("fs1")
    out += client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_describe_sessions():
    client, results_path = __init_test()

    client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)

    # creating a bunch of sessions
    client.create_session("fs0", "session0", "client0")
    client.create_session("fs0", "session1", "client1")
    client.reset_session(
        "fs0",
        "session0",
        "client0",
        "some session state".encode("utf-8"))
    client.reset_session(
        "fs0",
        "session1",
        "client1",
        "another session state".encode("utf-8"))

    out = client.execute_action("describesessions", {"FileSystemId": "fs0"})
    sessions = json.loads(out)

    with open(results_path, "w") as results_file:
        json.dump(sessions, results_file, indent=4)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_stat():
    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    client.mkdir("fs0", "/aaa")
    out = client.stat("fs0", "/aaa")
    stat = json.loads(out)
    del stat["ATime"]
    del stat["MTime"]
    del stat["CTime"]

    with open(results_path, "w") as results_file:
        json.dump(stat, results_file, indent=4)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_write_ls_rm_ls():
    client, results_path = __init_test()

    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write("some data")

    out = client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        BLOCKS_COUNT)

    out += client.ls("fs0", "/")
    client.write("fs0", "/xxx", "--data", data_file)
    out += __exec_ls(client, "fs0", "/")
    out += client.rm("fs0", "/xxx")
    out += __exec_ls(client, "fs0", "/")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret
