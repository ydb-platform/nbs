PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
PEERDIR(
    cloud/filestore/tests/python/lib
)

TEST_SRCS(
    test.py
)

SET(QEMU_VIRTIO fs)


INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tools/testing/fs_posix_compliance/fs_posix_compliance.inc)

SET(FILESTORE_SHARD_COUNT 2)
SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/common_configs/nfs-storage-newfeatures-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
