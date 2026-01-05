PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
SPLIT_FACTOR(1)

DEPENDS(
    cloud/blockstore/tools/testing/eternal_tests/eternal-load/bin
)

PEERDIR(
    cloud/filestore/tests/python/lib
)

TEST_SRCS(
    test.py
)

SET(QEMU_VIRTIO fs)
SET(NFS_FORCE_VERBOSE 1)
SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/common_configs/nfs-storage-eternal-load-patch.txt
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
