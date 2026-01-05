PY3TEST()

# This test uses qemu recipe that allocates a lot of memory and may fail with
# OOM under msan. It is disabled for now since:
# 1. there was no way found to exclude particular target from sanitizer build
# 2. max_allocation_size_mb option is ignored in release build for unknown reason
# Test fails under asan and tsan: https://github.com/ydb-platform/nbs/issues/3343
IF (SANITIZER_TYPE != "memory" AND SANITIZER_TYPE != "address" AND SANITIZER_TYPE != "thread")

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

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
    NFS_LOCAL_SERVICE_CONFIG_PATCH
    cloud/filestore/tests/fio/qemu-local-noserver-mq-test/local-service-patch.txt
)

SET(QEMU_VIRTIO fs)
SET(QEMU_ROOTFS cloud/storage/core/tools/testing/qemu/image-plucky/rootfs.img)
SET(QEMU_NUM_REQUEST_QUEUES 8)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-local-noserver.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

ENDIF()

END()
