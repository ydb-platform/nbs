PY3TEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ENDIF()

DEPENDS(
    cloud/filestore/tools/analytics/profile_tool
    cloud/storage/core/tools/testing/fio/bin
    cloud/storage/core/tools/testing/qemu/image-plucky
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/filestore/tools/testing/profile_log
    cloud/storage/core/tools/testing/fio/lib
)

TEST_SRCS(
    test.py
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/fio/qemu-kikimr-mq-test/nfs-patch.txt
)

SET(QEMU_VIRTIO fs)
SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-plucky/rootfs.img)
SET(QEMU_NUM_REQUEST_QUEUES 8)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
