import tempfile
import os
from subprocess import call
import yatest.common as yatest_common


def test_load(load_bin=None, tmp_dir=None):
    if tmp_dir is None:
        tmp_dir = yatest_common.ram_drive_path()

    block_size = 4096
    block_count_per_device = 262144  # 1 GiB

    tmp_file = tempfile.NamedTemporaryFile(suffix=".nonrepl", delete=False, dir=tmp_dir)

    tmp_file.seek(block_count_per_device * block_size - 1)
    tmp_file.write(b'\0')
    tmp_file.flush()

    if load_bin is None:
        load_bin = yatest_common.binary_path("cloud/blockstore/tools/testing/NBSNEBIUS-972/load")

    result = call([load_bin, tmp_file.name])

    tmp_file.close()
    os.unlink(tmp_file.name)

    assert result == 0


if __name__ == '__main__':
    test_load('../load', '.')
