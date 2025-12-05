import contrib.ydb.tests.library.common.yatest_common as yatest_common

import os

def get_ydb_binary_path():
    return yatest_common.binary_path(os.getenv("YDBD_BINARY"))
