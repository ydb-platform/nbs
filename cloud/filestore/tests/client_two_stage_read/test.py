import os

import yatest.common as common
import filecmp
import random
import string
import logging

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

    file_size = 1024*1024
    num_writes = random.randint(1, 10)

    client, results_path = __init_test()
    client.create("fs0", "test_cloud", "test_folder")
    expected_file = os.path.join(common.output_path(), "expected_data.txt")
    with open(expected_file, "w") as expected_file_handle:
        for i in range(num_writes):
            write_size = random.randint(1, 1000) // 2 * 2
            offset = random.randint(0, file_size - write_size) // 2 * 2
            data = ''.join(random.choices(string.ascii_letters, k=write_size))
            expected_file_handle.seek(offset)
            expected_file_handle.write(data)

            data_file = os.path.join(common.output_path(), "data.txt")
            with open(data_file, "w") as data_file_handle:
                data_file_handle.write(data)
            client.write("fs0", "/aaa", "--data", data_file, "--offset", f"{offset}")

    result = client.read("fs0", "/aaa", "--length", f"{file_size}").decode("utf8")

    client.destroy("fs0")

    with open(results_path, "w") as results_file:
        results_file.write(result)

    assert filecmp.cmp(expected_file, results_path)
