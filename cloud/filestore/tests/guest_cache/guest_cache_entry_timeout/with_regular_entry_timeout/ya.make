PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
SPLIT_FACTOR(1)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/tools/analytics/profile_tool
)

PEERDIR(
    cloud/filestore/tests/guest_cache/guest_cache_entry_timeout/lib
)


SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/guest_cache/guest_cache_entry_timeout/with_regular_entry_timeout/nfs-storage-patch.txt
)

SET(QEMU_VIRTIO fs)
SET(QEMU_INVOKE_TEST NO)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
