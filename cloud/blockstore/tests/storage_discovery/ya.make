PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

END()
