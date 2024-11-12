import cloud.filestore.tools.testing.fs_posix_compliance.pylib.test as compliance
import cloud.filestore.tools.testing.profile_log.common as profile

import yatest.common as common

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path


def test_profile_log():
    for key, value in compliance.get_kikimr_service_suites().items():
        compliance.run_compliance_suite(get_filestore_mount_path(), key, value)

    fs_name = "nfs_test"
    results_path = common.output_path("results.txt")
    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool")

    profile.dump_profile_log(profile_tool_bin_path,
                             common.output_path("nfs-profile.log"),
                             fs_name,
                             "NFS profile",
                             results_path)
    profile.dump_profile_log(profile_tool_bin_path,
                             common.output_path("vhost-profile.log"),
                             fs_name,
                             "VHOST profile",
                             results_path)

    ret = common.canonical_file(results_path, local=True)
    return ret
