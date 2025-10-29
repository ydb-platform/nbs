PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)
TEST_SRCS(timeout_test.py)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    library/python/testing/yatest_common

    ydb/tests/library

    contrib/python/requests/py3
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/blockstore/apps/disk_agent
    ydb/apps/ydbd
)

END()
