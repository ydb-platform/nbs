PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
SPLIT_FACTOR(1)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/fmdtest/bin
)

PEERDIR(
    cloud/filestore/tests/fmdtest/lib
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/fmdtest/configs/nfs-storage-multi-client.txt
)

SET(QEMU_VIRTIO fs)
SET(QEMU_INSTANCE_COUNT 2)
SET(FILESTORE_VHOST_ENDPOINT_COUNT 2)
SET(VIRTIOFS_SERVER_COUNT 2)
SET(QEMU_INVOKE_TEST NO)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
