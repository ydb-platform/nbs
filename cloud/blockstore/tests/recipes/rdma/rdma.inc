INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(1)

SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img)
DEPENDS(cloud/storage/core/tools/testing/qemu/image-noble)

SET(QEMU_INSTANCE_COUNT 2)
SET(QEMU_INVOKE_TEST NO)
SET(VIRTIOFS_SERVER_COUNT 2)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)
