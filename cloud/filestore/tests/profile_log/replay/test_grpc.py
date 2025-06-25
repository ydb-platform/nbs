import os
import pytest
import pathlib
import yatest.common as common

from cloud.filestore.tools.testing.loadtest.protos.loadtest_pb2 import TTestGraph
from google.protobuf.text_format import MessageToString

tests = ["sqlite", "jpeg", "fstest"]

bindir = pathlib.Path(common.build_path("cloud/filestore/tests/profile_log/replay/data"))


def run_replay(name):
    dir_out_path = os.getenv("NFS_MOUNT_PATH")
    tool_conf_path = str(pathlib.Path(common.output_path()) / "config.txt")

    config = TTestGraph()
    config.Tests.add()
    config.Tests[0].LoadTest.Name = "test"
    config.Tests[0].LoadTest.KeepFileStore = True
    config.Tests[0].LoadTest.ReplayGrpcSpec.FileName = str(bindir / (name + ".log"))
    config.Tests[0].LoadTest.IODepth = 1 # TODO: fix flaps and set 4-16
    config.Tests[0].LoadTest.CreateFileStoreRequest.FileSystemId = "nfs_share"
    config.Tests[0].LoadTest.CreateFileStoreRequest.CloudId = "cloud"
    config.Tests[0].LoadTest.CreateFileStoreRequest.FolderId = "folder"
    config.Tests[0].LoadTest.CreateFileStoreRequest.BlockSize = 4096
    config.Tests[0].LoadTest.CreateFileStoreRequest.BlocksCount = 10241024
    with open(tool_conf_path, "w") as config_file:
        config_file.write(MessageToString(config))
        config_file.flush()

    tool_bin_path = common.binary_path("cloud/filestore/tools/testing/loadtest/bin/filestore-loadtest")
    common.execute([tool_bin_path,
                    "--tests-config", tool_conf_path,
                    "--port", os.getenv("NFS_SERVER_PORT"),
                    ])

    proc = common.execute(["bash", "-xc", " cd " + dir_out_path + " && find . -type f -iname '*' -printf '%h/%f %s \n' | sort "])
    return proc.stdout.decode('utf-8')


@pytest.mark.parametrize("name", tests)
def test_profile_log(name):
    results_path = common.output_path("results.txt")
    result = run_replay(name)

    with open(results_path, 'w') as results:
        results.write(result)

    ret = common.canonical_file(results_path, local=True)
    return ret
