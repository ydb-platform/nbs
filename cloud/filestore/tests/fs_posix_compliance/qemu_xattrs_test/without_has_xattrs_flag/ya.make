PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
SPLIT_FACTOR(1)

TEST_SRCS(
    test.py
)

PEERDIR(
    cloud/filestore/tests/fs_posix_compliance/qemu_xattrs_test/lib
)

SET(QEMU_VIRTIO fs)
SET(QEMU_INVOKE_TEST NO)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)

SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img)
DEPENDS(cloud/storage/core/tools/testing/qemu/image-noble)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
