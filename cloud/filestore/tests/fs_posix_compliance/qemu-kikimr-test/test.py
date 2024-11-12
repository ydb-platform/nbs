import pytest

import cloud.filestore.tools.testing.fs_posix_compliance.pylib.test as compliance
import yatest.common as common

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path

__suites = compliance.get_kikimr_service_suites()


@pytest.mark.parametrize("suite", __suites.keys())
def test_posix_compliance(suite):
    results_path = common.output_path() + "/results-{}.txt".format(suite)
    with open(results_path, 'w') as results:
        out = compliance.run_compliance_suite(get_filestore_mount_path(), suite, __suites[suite])
        results.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret
