PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

DEPENDS(
    cloud/storage/core/tools/testing/fio/bin
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/storage/core/tools/testing/fio/lib
)

TEST_SRCS(
    test.py
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/common_configs/nfs-storage-newfeatures-patch.txt
)
SET(NFS_BS_FAILURE_PROBABILITY 0.01)

SET(
    NFS_SERVICE_CONFIG_PATCH
    cloud/filestore/tests/common_configs/nfs-vhost-newfeatures-patch.txt
)

SET(FILESTORE_VHOST_QUEUE_COUNT 8)

SET(QEMU_VIRTIO fs)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

DEFAULT(FILESTORE_TABLETS_RESTART_INTERVAL 5)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/tablets-restarter.inc)

END()
