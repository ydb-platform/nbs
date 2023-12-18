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

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-local.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-local.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
