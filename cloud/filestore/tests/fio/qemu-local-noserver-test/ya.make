PY3TEST()

# This test uses qemu recipe that allocates a lot of memory and may fail with
# OOM under msan. It is disabled for now since:
# 1. there was no way found to exclude particular target from sanitizer build
# 2. max_allocation_size_mb option is ignored in release build for unknown reason
# Test fails under asan: https://github.com/ydb-platform/nbs/issues/3343
IF (SANITIZER_TYPE != "memory" AND SANITIZER_TYPE != "address")

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
SET(VHOST_RESTART_INTERVAL 10)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-local-noserver.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

ENDIF()

END()
