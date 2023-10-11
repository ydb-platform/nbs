import yatest.common as common
from yatest.common import process


def test_load():
    binary_path = common.binary_path(
        "cloud/disk_manager/test/loadtest/cmd/cmd")
    dm_config = common.get_param("disk-manager-client-config")
    assert dm_config is not None

    cmd = [
        binary_path,
        "--disk-manager-client-config", common.source_path(dm_config),
    ]
    process.execute(cmd)
