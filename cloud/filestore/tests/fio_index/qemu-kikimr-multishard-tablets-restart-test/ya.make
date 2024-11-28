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

SET(QEMU_VIRTIO fs)
SET(FILESTORE_SHARD_COUNT 5)
SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/loadtest/service-kikimr-newfeatures-test/nfs-storage.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

SET(FILESTORE_TABLETS_RESTART_INTERVAL 5)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/tablets-restarter.inc)

END()
