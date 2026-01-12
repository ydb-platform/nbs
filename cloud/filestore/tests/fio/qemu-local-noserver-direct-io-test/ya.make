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

SET(
    NFS_LOCAL_SERVICE_CONFIG_PATCH
    cloud/filestore/tests/fio/qemu-local-noserver-direct-io-test/local-service-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-local-noserver.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
