PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/medium.inc)

TEST_SRCS(test.py)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    library/python/testing/yatest_common

    ydb/tests/library

    contrib/python/requests
)

DEPENDS(
    cloud/blockstore/apps/server
    ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

END()
