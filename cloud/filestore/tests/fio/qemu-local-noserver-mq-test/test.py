import pytest

import cloud.filestore.tools.testing.profile_log.common as profile
import cloud.storage.core.tools.testing.fio.lib as fio

import yatest.common as common

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path

import sys


TESTS = fio.generate_tests(iodepths=[32], duration=30)


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):
    mount_dir = get_filestore_mount_path()
    file_name = fio.get_file_name(mount_dir, name)

    fio.run_test(file_name, TESTS[name], fail_on_errors=True)

    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool")
    fs_name = "nfs_test"
    events = profile.get_profile_log_events(
        profile_tool_bin_path,
        common.output_path("vhost-profile.log"),
        fs_name)

    #
    # Calculating the number of threads that were actually involved in request
    # processing.
    #

    thread_ids = set()
    for event in events:
        loop_thread_id = event[1].get("loop_thread_id")
        if loop_thread_id is not None:
            thread_ids.add(loop_thread_id)

    #
    # Usually the number of threads involved is 7 or 8 but sometimes its less
    # so let's use a coarse check (> 1) in order not to have an unstable test.
    # The main thing that we want to verify is that multiqueue actually works
    # which means that more than 1 thread should be involved in request
    # processing.
    #

    print("loop_thread_count=%s" % len(thread_ids), file=sys.stderr)
    assert len(thread_ids) > 1
