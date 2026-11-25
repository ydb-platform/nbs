PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    cloud/storage/core/tools/testing/ydb/bin
)

PEERDIR(
    cloud/blockstore/tests/python/lib
    cloud/fastshard/public/api/protos
)

END()
