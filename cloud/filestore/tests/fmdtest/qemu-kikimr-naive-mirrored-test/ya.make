PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/large.inc)
SPLIT_FACTOR(1)

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
    cloud/filestore/tests/fmdtest/configs/nfs-storage-persistent-fastshard.txt
)

# Naive-mirrored talks to the disk-agent's journalled_device_tcp_server,
# which is the SCT_TCP side channel. Vhost must be told to bring up silk
# and open a fast-shard port for the tablet <-> shard control path.
SET(
    NFS_SERVICE_CONFIG_PATCH
    cloud/filestore/tests/fmdtest/configs/vhost-sidechannel.txt
)
SET(USE_FAST_SHARD_PORT yes)

SET(QEMU_VIRTIO fs)
SET(QEMU_INSTANCE_COUNT 1)
SET(FILESTORE_VHOST_ENDPOINT_COUNT 1)
SET(FILESTORE_BLOCKS_COUNT 524288)
SET(VIRTIOFS_SERVER_COUNT 1)
SET(QEMU_INVOKE_TEST NO)

# Using 1 device to make the test stable. The current implementation of
# mirroring is a prototype and doesn't guarantee atomicity and consistency so
# with 3 devices we might run into some inconsistencies between different data
# structures from time to time when one data structure page is read from one
# device and the other - from another device.
SET(FASTSHARD_DA_DEVICE_COUNT 1)
# 1GiB should be more than enough.
SET(FASTSHARD_DA_DEVICE_SIZE 1073741824)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/blockstore-disk-agent.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-kikimr.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/vhost-endpoint.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
