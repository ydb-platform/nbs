INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

REQUIREMENTS(
    container:5897560351  # container with tcp_tw_reuse = 1
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/blockstore/tools/testing/loadtest/bin

    contrib/ydb/apps/ydb
    contrib/ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    contrib/ydb/core/protos
    contrib/ydb/tests/library
    cloud/blockstore/pylibs/ydb/tests/library
)
