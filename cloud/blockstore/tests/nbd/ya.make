PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TEST_SRCS(test.py)

DEPENDS(
    cloud/blockstore/apps/server
    contrib/ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/tests/python/lib
)

SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img)
DEPENDS(cloud/storage/core/tools/testing/qemu/image-noble)
SET_APPEND(QEMU_INVOKE_TEST YES)
SET_APPEND(QEMU_VIRTIO none)
SET_APPEND(QEMU_ENABLE_KVM True)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
