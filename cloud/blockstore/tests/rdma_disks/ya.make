PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
)

IF (OPENSOURCE)
    DEPENDS(
        cloud/storage/core/tools/testing/ydb/bin
    )
ELSE()
    DEPENDS(
        contrib/ydb/apps/ydbd
    )
ENDIF()

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

END()
