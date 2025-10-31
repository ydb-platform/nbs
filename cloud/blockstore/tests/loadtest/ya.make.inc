INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

REQUIREMENTS(
    container:5897560351  # container with tcp_tw_reuse = 1
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/blockstore/tools/testing/loadtest/bin

    ydb/apps/ydb
    ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib

    ydb/core/protos
    ydb/tests/library
)
