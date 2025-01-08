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


def __write_some_data(client, fs_id, path, data):
    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write("data for %s" % path)
        f.write(":: actual data: %s" % data)

    client.write(fs_id, path, "--data", data_file)


class FsItem:

    def __init__(self, path, is_dir, data):
        self.path = path
        self.is_dir = is_dir
        self.data = data


def __fill_fs(client, fs_id, items):
    for item in items:
        if item.is_dir:
            client.mkdir(fs_id, item.path)
        else:
            if item.data is not None:
                __write_some_data(client, fs_id, item.path, item.data)
            else:
                client.touch(fs_id, item.path)


def test_nonsharded_vs_sharded_fs():
    client, results_path = __init_test()
    client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1)
    client.create(
        "fs1",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        3 * int(SHARD_SIZE / BLOCK_SIZE))

    def _d(path):
        return FsItem(path, True, None)

    def _f(path, data=None):
        return FsItem(path, False, data)

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
        _d("/a1"),
        _d("/a1/b1"),
        _d("/a1/b2"),
        _f("/a1/b2/f11.txt", "zzzzz"),
        _f("/a1/b2/f12.txt", "zzzzz2"),
        _f("/a1/b2/f13.txt", "zzzzz3"),
        _f("/a1/b2/f14.txt", "zzzzz4"),
        _d("/a1/b2/c1"),
        _f("/a1/b2/f15.txt", "ZZZZZZZZZZZ"),
        _d("/a1/b3"),
    ]

    __fill_fs(client, "fs0", items)
    __fill_fs(client, "fs1", items)

    out = __exec_ls(client, "fs0", "/", "--disable-multitablet-forwarding")
    out += __exec_ls(client, "fs1", "/", "--disable-multitablet-forwarding")
    out += client.diff("fs0", "fs1")

    client.destroy("fs0")
    client.destroy("fs1")

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret
