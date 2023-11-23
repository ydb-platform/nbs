INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(1)

SET(VIRTIOFS_SERVER_COUNT 2)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/virtiofs-server.inc)

SET(QEMU_ROOTFS cloud/blockstore/tests/rdma/image/rootfs.img)
DEPENDS(cloud/blockstore/tests/rdma/image)

SET(QEMU_INSTANCE_COUNT 2)
SET(QEMU_INVOKE_TEST NO)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)
