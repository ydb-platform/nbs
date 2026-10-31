PY3TEST()

TEST_SRCS(
    test_runner.py
    test_integration.py
)

DEPENDS(
    cloud/storage/core/tools/testing/virtiofs_server/bin
)

PEERDIR(
    library/python/testing/yatest_common
    cloud/storage/core/tools/testing/virtiofs_server/lib
)

END()
