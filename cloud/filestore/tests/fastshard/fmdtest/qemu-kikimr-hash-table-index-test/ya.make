PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
SPLIT_FACTOR(2)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tools/testing/fmdtest/bin
)

PEERDIR(
    cloud/filestore/public/sdk/python/client
    cloud/filestore/tests/python/lib

    cloud/storage/core/tools/testing/qemu/lib
)

SET(
    NFS_STORAGE_CONFIG_PATCH
    cloud/filestore/tests/fastshard/fmdtest/configs/nfs-storage-persistent-fastshard.txt
)

# Hash-table-index talks to the disk-agent's journalled_device_tcp_server,
# which is the SCT_TCP side channel. Vhost must be told to bring up silk
# and open a fast-shard port for the tablet <-> shard control path.
SET(
    NFS_SERVICE_CONFIG_PATCH
    cloud/filestore/tests/fastshard/fmdtest/configs/vhost-sidechannel.txt
)
SET(USE_FAST_SHARD_PORT yes)

SET(QEMU_VIRTIO fs)
SET(QEMU_INSTANCE_COUNT 1)
SET(FILESTORE_VHOST_ENDPOINT_COUNT 1)
SET(FILESTORE_BLOCKS_COUNT 524288)
SET(VIRTIOFS_SERVER_COUNT 1)
SET(QEMU_INVOKE_TEST NO)

# tests can use up to 3 devices
SET(FASTSHARD_DA_DEVICE_COUNT 3)
# 1GiB should be more than enough.
SET(FASTSHARD_DA_DEVICE_SIZE 1073741824)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/blockstore-disk-agent.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
