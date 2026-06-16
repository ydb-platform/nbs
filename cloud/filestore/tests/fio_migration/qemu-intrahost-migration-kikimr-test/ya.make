PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)

DEPENDS(
    cloud/storage/core/tools/testing/fio/bin
    cloud/storage/core/tools/testing/qemu/bin
    cloud/storage/core/tools/testing/qemu/image
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/storage/core/tools/common/python
    cloud/storage/core/tools/testing/fio/lib
    cloud/storage/core/tools/testing/qemu/lib
)

DATA(arcadia/cloud/storage/core/tools/testing/qemu/keys)

TEST_SRCS(
    test.py
)

SET(QEMU_VIRTIO fs)
SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/common_configs/nfs-storage-migration-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/virtiofs-server.inc)

END()

