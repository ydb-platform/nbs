PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/storage/core/tools/testing/ydb/bin
)

PEERDIR(
    cloud/blockstore/tests/python/lib
    cloud/blockstore/libs/local_nvme/protos
)

END()
