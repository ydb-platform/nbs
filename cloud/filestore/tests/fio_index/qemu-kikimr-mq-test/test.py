import sys

import pytest
import yatest.common as common

import cloud.filestore.tools.testing.profile_log.common as profile
import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path


TESTS = fio.generate_default_index_test()


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    mount_dir = get_filestore_mount_path()
    dir_name = fio.get_dir_name(mount_dir, name)

    fio.run_index_test(dir_name, TESTS[name], fail_on_errors=True)

    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool")
    fs_name = "nfs_test"
    events = profile.get_profile_log_events(
        profile_tool_bin_path,
        common.output_path("vhost-profile.log"),
        fs_name)

    thread_ids = set()
    for event in events:
        loop_thread_id = event[1].get("loop_thread_id")
        if loop_thread_id is not None:
            thread_ids.add(loop_thread_id)

    print("loop_thread_count=%s" % len(thread_ids), file=sys.stderr)
    assert len(thread_ids) > 1
