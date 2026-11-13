import os
import tempfile

import pytest

import cloud.filestore.tools.testing.fs_posix_compliance.pylib.test as compliance
import yatest.common as common

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path

__suites = compliance.get_local_service_suites(exclude=["xacl"])


@pytest.mark.parametrize("suite", __suites.keys())
def test_posix_compliance(suite):
    results_path = common.output_path() + "/results-{}.txt".format(suite)
    with open(results_path, 'w') as results:
        out = compliance.run_compliance_suite(get_filestore_mount_path(), suite, __suites[suite])
        results.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret


def test_unlinked_open_file():
    data = b"contents"
    with tempfile.TemporaryDirectory(dir=get_filestore_mount_path()) as directory:
        path = os.path.join(directory, "file")
        with open(path, "w+b", buffering=0) as file:
            assert file.write(data) == len(data)
            os.unlink(path)
            assert not os.path.exists(path)

            # fstat may be cached; ftruncate forces an inode-based service call.
            os.ftruncate(file.fileno(), len(data))
            stat = os.fstat(file.fileno())
            assert stat.st_size == len(data)
            assert stat.st_nlink == 0

            file.seek(0)
            assert file.read() == data
