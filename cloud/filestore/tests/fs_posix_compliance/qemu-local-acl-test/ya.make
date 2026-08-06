PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
SPLIT_FACTOR(1)

PEERDIR(
    cloud/filestore/tests/python/lib
)

TEST_SRCS(
    test.py
)

SET(QEMU_VIRTIO fs)
SET(
    NFS_LOCAL_SERVICE_CONFIG_PATCH
    cloud/filestore/tests/fs_posix_compliance/qemu-local-acl-test/local-service-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-local-noserver.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)

SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img)
DEPENDS(cloud/storage/core/tools/testing/qemu/image-noble)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
