PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

TEST_SRCS(
    keepalive.py
    test_with_client.py
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server

    ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/client
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    ydb/tests/library
)

END()
