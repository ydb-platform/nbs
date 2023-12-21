PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

DEPENDS(
    cloud/blockstore/apps/server
    ydb/apps/ydbd
)

END()
