PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)
SPLIT_FACTOR(1)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/bin
    cloud/storage/core/tools/testing/qemu/image-noble
)

SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img)
SET_APPEND(QEMU_INVOKE_TEST YES)
SET_APPEND(QEMU_VIRTIO none)
SET_APPEND(QEMU_ENABLE_KVM True)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
