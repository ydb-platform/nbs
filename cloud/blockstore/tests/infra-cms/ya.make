PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/blockstore/tools/testing/loadtest/bin
    cloud/storage/core/tools/testing/unstable-process
    contrib/ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/blockstore/tests/loadtest/local-nonrepl
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/public/sdk/python/client
    cloud/blockstore/tests/python/lib
    contrib/ydb/core/protos
    contrib/ydb/tests/library
    cloud/blockstore/pylibs/ydb/tests/library
)

END()
