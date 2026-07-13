import runpy

import yatest.common as common


test = runpy.run_path(common.source_path(
    "cloud/filestore/tests/async_open_test/test.py"))["test"]
