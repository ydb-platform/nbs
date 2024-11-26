PY3TEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ENDIF()

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

ENV(SANITIZER_TYPE=${SANITIZER_TYPE})
SET(QEMU_VIRTIO fs)
SET(NFS_RESTART_INTERVAL 10)
SET(VHOST_RESTART_INTERVAL 10)
SET(VHOST_RESTART_FLAG 1)
SET(FILESTORE_SHARD_COUNT 5)
SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/loadtest/service-kikimr-newfeatures-test/nfs-storage.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
