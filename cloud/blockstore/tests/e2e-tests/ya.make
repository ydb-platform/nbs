PY3TEST()

SIZE(MEDIUM)
TIMEOUT(600)
REQUIREMENTS(
    cpu:4
    ram:16
)


TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/endpoint_proxy
    cloud/blockstore/apps/server
    ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
    cloud/storage/core/protos
    ydb/core/protos
    ydb/tests/library
)
SET_APPEND(QEMU_INVOKE_TEST YES)
SET_APPEND(QEMU_VIRTIO none)
SET_APPEND(QEMU_ENABLE_KVM True)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
