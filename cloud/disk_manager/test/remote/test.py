import library.python.testing.yatest_common.yatest.common as common
import library.python.testing.yatest_common.yatest.common.process as process

def test_remote():
    binary_path = common.binary_path(
        "cloud/disk_manager/test/remote/cmd/cmd")
    dm_config = common.get_param("disk-manager-client-config")
    assert dm_config is not None
    nbs_config = common.get_param("nbs-client-config")
    assert nbs_config is not None
    test_config = common.get_param("test-config")
    assert test_config is not None

    cmd = [
        binary_path,
        "--disk-manager-client-config", common.source_path(dm_config),
        "--nbs-client-config", common.source_path(nbs_config),
        "--test-config", common.source_path(test_config),
    ]
    process.execute(cmd)
