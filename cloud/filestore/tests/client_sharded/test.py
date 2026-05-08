import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.common import fetch_counters, filter_counters
from cloud.filestore.tests.python.lib.fs import (
    FsItem,
    fill_fs,
    DIR,
    FILE,
    SYMLINK,
    fetch_dir_viewer_entries,
    fetch_locks,
)

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
    client, _, results_path = __init_test()
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
    client, _, results_path = __init_test()
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


def test_enable_strict():
    client, _, results_path = __init_test()
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
        enable_strict=True)
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

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_enable_directory_creation_in_shards():
    client, _, results_path = __init_test()
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
        enable_directory_creation_in_shards=True)
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

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_force_directory_creation_in_shards():
    client, client_nocheck, results_path = __init_test()
    block_count = 3 * int(SHARD_SIZE / BLOCK_SIZE)
    client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        block_count)

    def _d(path):
        return FsItem(path, DIR, None)

    def _f(path, data=None):
        return FsItem(path, FILE, data)

    def _l(path, symlink):
        return FsItem(path, SYMLINK, symlink)

    items = [
        _d("/a0"),
        _f("/a0/f0.txt", "xxx"),
        _f("/a0/f1.txt", "xxx2"),
        _f("/a0/f2.txt", "xxx3"),
        _d("/a0/b0"),
        _f("/a0/f3.txt", "xxx4"),
        _f("/a0/f4.txt"),
        _d("/a0/b0/c0"),
        _f("/a0/f5.txt"),
        _f("/a0/f6.txt", "yyyy"),
        _f("/a0/f7.txt", "yyyy2"),
        _f("/a0/f8.txt"),
        _d("/a0/b0/c0/d0"),
        _f("/a0/b0/c0/d0/f9.txt", "yyyy3"),
        _f("/a0/b0/c0/d0/f10.txt", "yyyy4"),
    ]

    fill_fs(client, "fs0", items)

    out = client.resize(
        "fs0",
        block_count,
        enable_directory_creation_in_shards=True,
        force_directory_creation_in_shards=True)

    items = [
        _d("/a1"),
        _d("/a1/b1"),
        _d("/a1/b2"),
        _f("/a1/b2/f11.txt", "zzzzz"),
        _f("/a1/b2/f12.txt", "zzzzz2"),
        _f("/a1/b2/f13.txt", "zzzzz3"),
        _f("/a1/b2/f14.txt", "zzzzz4"),
        _d("/a1/b2/c1"),
        _f("/a1/b2/f15.txt", "ZZZZZZZZZZZ"),
        _l("/a1/b2/l1", "/does/not/matter"),
        _f("/a1/b2/f16.txt", "ZZZZZZZZZZZ2"),
        _f("/f17.txt", "010101010101010101"),
        _f("/a1/f18.txt", "xxxxxxxxxxxxxxxxxxxxxxxxxx"),
        _d("/a1/b3"),
        _d("/a1/b4"),
        _f("/a1/b4/f19.txt", "zzz"),
        _d("/a1/b4/c2"),
        _d("/a1/b5"),
        _d("/a1/b6"),
        _f("/a1/b6/f20.txt", "yyyyy"),
        _f("/.something"),
        _f("/another.something"),
        _f("/file.txt"),
    ]

    fill_fs(client, "fs0", items)

    # checking that mv, rm and ln work properly
    client.mv("fs0", "/a0/b0/c0/d0/f9.txt", "/a0/b0/c0/d0/f9_moved.txt")
    client.rm("fs0", "/a1/b2/f16.txt")
    client.mv("fs0", "/a1/b4", "/a1/b5")
    client_nocheck.mv("fs0", "/a1/b5", "/a1/b6")
    # checking that cross-directory mv works
    client.mv("fs0", "/f17.txt", "/a0/f17.txt")
    client.mv("fs0", "/a0/f17.txt", "/a0/b0/f17.txt")
    client.mv("fs0", "/a0/b0/f17.txt", "/a0/b0/c0/f17.txt")
    client.mv("fs0", "/a0/b0/c0/f17.txt", "/a0/b0/c0/d0/f17.txt")
    client.mv("fs0", "/a0/b0/c0/d0/f17.txt", "/a1/f17.txt")
    client.mv("fs0", "/a1/f17.txt", "/a1/b2/f17.txt")
    # local-to-external mv shouldn't succeed
    client_nocheck.mv("fs0", "/a0/b0", "/a1/b0")
    # checking that cross-directory mv to root works
    client.mv("fs0", "/a1/f18.txt", "/f18.txt")
    # checking that readlink works (indirectly - via diff)
    client.ln("fs0", "/a1/b2/l2", "--symlink", "/does/not/matter/2")

    out += __exec_ls(client, "fs0", "/", "--disable-multitablet-forwarding")
    out += __exec_ls(client, "fs0", "/a1/")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    #
    # Querying dirViewer for the root node, dumping the result w/o unstable
    # fields.
    #

    tablet_id = json.loads(client.describe("fs0"))["FileStore"]["MainTabletId"]
    root_node_id = 1
    entries = fetch_dir_viewer_entries(tablet_id, root_node_id)
    result = json.dumps(entries, indent=4)
    with open(results_path, 'a') as results:
        results.write('dirViewer(1):\n')
        results.write('{}\n'.format(result))

    #
    # And let's do the same thing for locks viewer - there should be no locks
    # so this is just a smoke test.
    #

    locks = fetch_locks(tablet_id)
    result = json.dumps(locks, indent=4)
    with open(results_path, 'a') as results:
        results.write('locks():\n')
        results.write('{}\n'.format(result))

    #
    # Canonizing some counters.
    #

    counters = fetch_counters()
    filtered_counters = filter_counters(
        counters,
        "fs0",
        ["RenameNotSupportedErrorCount"])
    with open(results_path, 'a') as results:
        results.write('counters():\n')
        results.write('{}\n'.format(json.dumps(filtered_counters)))

    #
    # Destroying the filesystem - important to do it after querying dirViewer.
    #

    client.destroy("fs0")

    ret = common.canonical_file(results_path, local=True)
    return ret
