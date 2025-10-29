PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    cloud/blockstore/tools/testing/fake-vhost-server
    cloud/blockstore/vhost-server
    ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

END()
