PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

DEPENDS(
    cloud/blockstore/tools/testing/rdma-client-cq-stall/bin
)

TEST_SRCS(
    test.py
)

SET_APPEND(QEMU_INVOKE_TEST YES)
SET_APPEND(QEMU_VIRTIO none)
SET_APPEND(QEMU_ENABLE_KVM True)
SET_APPEND(QEMU_MEM 8G)
SET_APPEND(QEMU_PROC 4)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
