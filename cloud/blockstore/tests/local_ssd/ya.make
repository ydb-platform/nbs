PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
    test_allocation_retry.py
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    contrib/ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

END()
