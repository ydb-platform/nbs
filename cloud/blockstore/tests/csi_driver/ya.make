PY3TEST()

IF (OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
ENDIF()


SPLIT_FACTOR(1)
TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/csi_driver/cmd/nbs-csi-driver
    cloud/blockstore/tools/csi_driver/client
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/endpoint_proxy
    cloud/blockstore/apps/server
    contrib/ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
    cloud/storage/core/protos
    contrib/ydb/core/protos
    contrib/ydb/tests/library
)
SET_APPEND(QEMU_INVOKE_TEST YES)
SET_APPEND(QEMU_VIRTIO none)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/csi_driver/recipe/recipe.inc)

END()
