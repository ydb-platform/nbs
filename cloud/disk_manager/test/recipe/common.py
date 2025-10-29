import contrib.ydb.tests.library.common.yatest_common as yatest_common
from yatest_lib.ya import TestMisconfigurationException


def get_ydb_binary_path():
    try:
        path = yatest_common.binary_path("cloud/storage/core/tools/testing/ydb/bin/ydbd")
    except TestMisconfigurationException:
        path = None

    if path is None:
        path = yatest_common.binary_path("ydb/apps/ydbd/ydbd")

    return path
