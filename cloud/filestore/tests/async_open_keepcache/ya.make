PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
SPLIT_FACTOR(1)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/testing/stale_read_loop
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/storage/core/tools/testing/qemu/lib
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/async_open_keepcache/nfs-storage-patch.txt
)

SET(QEMU_VIRTIO fs)
SET(QEMU_INVOKE_TEST NO)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
