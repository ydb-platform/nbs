PY3TEST()

TEST_SRCS(
    test_integration.py
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/apps/vhost
)

PEERDIR(
    library/python/testing/yatest_common
    cloud/storage/core/tools/testing/virtiofs_server/lib
)

END()
