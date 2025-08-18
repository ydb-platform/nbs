import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient


def __init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())

    results_path = common.output_path() + "/results.txt"
    return client, results_path


def test_unaligned_read():
    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write('a' * 10)
        f.write('b' * 10)

    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder")
    client.write("fs0", "/aaa", "--data", data_file)
    result = client.read("fs0", "/aaa", "--length", "10", "--offset", "10").decode("utf8")

    client.destroy("fs0")

    with open(results_path, "w") as results_file:
        results_file.write(result + '\n')

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_zero_range_read():
    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write('a' * 10)
        f.write('b' * 10)

    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder")
    client.write("fs0", "/aaa", "--data", data_file, "--offset", "10")
    client.write("fs0", "/aaa", "--data", data_file, "--offset", "40")
    result = client.read("fs0", "/aaa", "--length", "120").decode("utf8")

    client.destroy("fs0")

    with open(results_path, "w") as results_file:
        results_file.write(result + '\n')

    ret = common.canonical_file(results_path, local=True)
    return ret
