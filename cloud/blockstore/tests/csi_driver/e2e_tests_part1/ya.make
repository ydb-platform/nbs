PY3TEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
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
    cloud/blockstore/tests/csi_driver/lib
    cloud/blockstore/tools/testing/csi-sanity/bin
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/csi_driver/lib
    cloud/blockstore/tests/python/lib
    cloud/storage/core/protos
    ydb/core/protos
    ydb/tests/library
)
SET_APPEND(QEMU_INVOKE_TEST YES)
SET_APPEND(QEMU_VIRTIO none)
SET_APPEND(QEMU_ENABLE_KVM True)
SET_APPEND(QEMU_MEM 8G)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
