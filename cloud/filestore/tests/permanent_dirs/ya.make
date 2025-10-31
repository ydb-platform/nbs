PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
SPLIT_FACTOR(1)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/directory_handles_state_test
)

PEERDIR(
    cloud/filestore/public/sdk/python/client
    cloud/filestore/tests/python/lib

    cloud/storage/core/tools/testing/qemu/lib
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/common_configs/nfs-storage-newfeatures-patch.txt
)

SET(QEMU_VIRTIO fs)
SET(QEMU_INSTANCE_COUNT 1)
SET(FILESTORE_VHOST_ENDPOINT_COUNT 1)
SET(VIRTIOFS_SERVER_COUNT 1)
SET(QEMU_INVOKE_TEST NO)

SET(VHOST_RESTART_INTERVAL 10)
SET(VHOST_RESTART_FLAG 1)
SET(VHOST_RESTART_FLAG_ON_DEMAND YES)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
