PY3TEST()

PEERDIR(
    cloud/disk_manager/test/snapshot_backup
    library/python/testing/yatest_common
)

DEPENDS(contrib/tools/python3/bin)

PY_SRCS(
    helpers.py
    zstd_vectors.py
)

TEST_SRCS(
    test_cloud.py
    test_config.py
    test_devices.py
    test_reader.py
    test_runner.py
    test_state.py
    test_transport.py
)

SIZE(SMALL)

DATA(
    arcadia/cloud/disk_manager/test/snapshot_backup/transport.py
)

END()
