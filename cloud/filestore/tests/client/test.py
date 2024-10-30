import json
import os
import re

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

    client.destroy_session("fs0", "session0", "client0")
    client.destroy_session("fs0", "session1", "client1")
    client.destroy("fs0")

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

    client.destroy("fs0")

    with open(results_path, "w") as results_file:
        json.dump(stat, results_file, indent=4)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_ls():
    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)

    out = client.ls("fs0", "/")
    client.mkdir("fs0", "/aaa")
    out += client.ls("fs0", "/")
    out += client.ls("fs0", "/aaa")
    client.mkdir("fs0", "/bbb")
    client.touch("fs0", "/first")
    node_id = json.loads(client.stat("fs0", "/first"))["Id"]
    client.set_node_attr(
        "fs0", node_id, "--uid", 10, "--gid", 20, "--size", 123, "--mode", 221
    )
    out += client.ls("fs0", "/")
    # replace timestamps with a constant value
    out = re.sub(
        rb"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)",
        b"1970-01-01T00:00:00Z",
        out,
    )
    client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

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

    client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_set_node_attr():
    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    client.mkdir("fs0", "/aaa")

    out = client.stat("fs0", "/aaa")
    stat = json.loads(out)
    node_id = stat["Id"]
    uid = 1
    gid = 1
    size = 123
    mode = 221
    atime = stat["ATime"] - 1
    mtime = stat["MTime"] - 1
    ctime = stat["CTime"] - 1

    client.set_node_attr(
        "fs0", node_id,
        "--uid", uid,
        "--gid", gid,
        "--size", size,
        "--mode", mode,
        "--atime", atime,
        "--mtime", mtime,
        "--ctime", ctime)

    out = client.stat("fs0", "/aaa")
    stat = json.loads(out)

    client.destroy("fs0")

    assert uid == stat["Uid"]
    assert gid == stat["Gid"]
    assert size == stat["Size"]
    assert mode == stat["Mode"]
    assert atime == stat["ATime"]
    assert mtime == stat["MTime"]
    assert ctime == stat["CTime"]


def test_partial_set_node_attr():
    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    client.mkdir("fs0", "/aaa")
    client.touch("fs0", "/aaa/bbb")

    out = client.stat("fs0", "/aaa/bbb")
    stat = json.loads(out)
    node_id = stat["Id"]
    uid = 1
    gid = 1

    client.set_node_attr(
        "fs0", node_id,
        "--uid", uid,
        "--gid", gid,
        "--size", 123)

    out = client.stat("fs0", "/aaa/bbb")
    stat = json.loads(out)

    assert uid == stat["Uid"]
    assert gid == stat["Gid"]
    gid = 2
    client.set_node_attr(
        "fs0", node_id,
        "--gid", gid)
    out = client.stat("fs0", "/aaa/bbb")
    new_stat = json.loads(out)

    client.destroy("fs0")

    assert gid == new_stat["Gid"]
    assert stat["Uid"] == new_stat["Uid"]
    assert stat["Size"] == new_stat["Size"]
    assert stat["Mode"] == new_stat["Mode"]


def test_resize():
    client, results_path = __init_test()
    out = client.create(
        "fs0", "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT
    )

    out += client.resize("fs0", 2 * BLOCKS_COUNT)

    try:
        client.resize("fs0", BLOCKS_COUNT)
    except Exception:
        out += b'resize failed as expected'

    out += client.resize("fs0", BLOCKS_COUNT, force=True)
    out += client.destroy("fs0")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_multitablet_ls():
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

    out += client.create(
        "fs0-shard",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        BLOCKS_COUNT)

    out += client.execute_action("configureasshard", {
        "FileSystemId": "fs0-shard",
        "ShardNo": 1,
    })

    out += client.execute_action("configureshards", {
        "FileSystemId": "fs0",
        "ShardFileSystemIds": ["fs0-shard"],
    })

    client.write("fs0", "/xxx", "--data", data_file)
    out += __exec_ls(client, "fs0", "/")
    out += __exec_ls(client, "fs0", "/", "--disable-multitablet-forwarding")

    client.destroy("fs0")
    client.destroy("fs0-shard")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_multitablet_findgarbage():
    client, results_path = __init_test()

    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write("some data")

    big_data_file = os.path.join(common.output_path(), "big_data.txt")
    with open(big_data_file, "w") as f:
        for i in range(1024):
            f.write("some big data %s\n" % i)

    fs_id = "fs0"
    shard1_id = fs_id + "-shard1"
    shard2_id = fs_id + "-shard2"

    out = client.create(
        fs_id,
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        BLOCKS_COUNT)

    out += client.create(
        shard1_id,
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        BLOCKS_COUNT)

    out += client.create(
        shard2_id,
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        BLOCKS_COUNT)

    out += client.execute_action("configureasshard", {
        "FileSystemId": shard1_id,
        "ShardNo": 1,
    })

    out += client.execute_action("configureasshard", {
        "FileSystemId": shard2_id,
        "ShardNo": 2,
    })

    out += client.execute_action("configureshards", {
        "FileSystemId": fs_id,
        "ShardFileSystemIds": [shard1_id, shard2_id],
    })

    # let's generate multiple "pages" for listing
    for i in range(100):
        client.write(fs_id, "/xxx%s" % i, "--data", data_file)
    client.write(shard1_id, "/garbage1_1", "--data", data_file)
    client.write(shard2_id, "/garbage2_1", "--data", data_file)
    client.write(shard2_id, "/garbage2_2", "--data", data_file)
    client.write(shard2_id, "/garbage2_3", "--data", big_data_file)
    # TODO: teach the client to fetch shard list by itself
    out += client.find_garbage(fs_id, [shard1_id, shard2_id], page_size=1024)

    client.destroy(fs_id)
    client.destroy(shard1_id)
    client.destroy(shard2_id)

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_large_file():
    TiB = 1024 * 1024 * 1024 * 1024

    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write("some data")

    client, results_path = __init_test()
    client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        TiB // BLOCK_SIZE)
    client.mkdir("fs0", "/aaa")
    client.touch("fs0", "/aaa/bbb")

    out = client.stat("fs0", "/aaa/bbb")
    stat = json.loads(out)
    node_id = stat["Id"]
    result = json.dumps(__process_stat(stat))
    result += "\n"

    client.write("fs0", "/aaa/bbb", "--data", data_file)

    client.set_node_attr("fs0", node_id, "--size", str(TiB))
    result += client.read("fs0", "/aaa/bbb", "--length", "9").decode("utf8")
    result += "\n"

    out = client.stat("fs0", "/aaa/bbb")
    stat = json.loads(out)
    assert stat["Size"] == TiB
    result += json.dumps(__process_stat(stat))
    result += "\n"

    client.set_node_attr("fs0", node_id, "--size", "1024")
    result += client.read("fs0", "/aaa/bbb", "--length", "0").decode("utf8")
    result += "\n"

    out = client.stat("fs0", "/aaa/bbb")
    new_stat = json.loads(out)
    assert stat["Size"] == TiB
    result += json.dumps(__process_stat(new_stat))
    result += "\n"
    result += client.read("fs0", "/aaa/bbb", "--length", "0").decode("utf8")
    result += "\n"

    client.rm("fs0", "/aaa/bbb")

    client.destroy("fs0")

    with open(results_path, "w") as results_file:
        results_file.write(result)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_forced_compaction():
    data_file = os.path.join(common.output_path(), "data.txt")
    chunk_size = 128 * 1024
    chunk = []
    for i in range(chunk_size):
        chunk.append("a")
    chunk_str = "".join(chunk)
    with open(data_file, "w") as f:
        f.write(chunk_str)

    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder")

    for i in range(128):
        client.write(
            "fs0",
            "/aaa",
            "--data", data_file,
            "--offset", str(i * chunk_size))

    result = client.forced_compaction("fs0").decode("utf8")

    client.destroy("fs0")

    with open(results_path, "w") as results_file:
        results_file.write(result)

    ret = common.canonical_file(results_path, local=True)
    return ret
