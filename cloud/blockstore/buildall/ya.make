PACKAGE()

PEERDIR(
    ydb/apps/ydb
    ydb/apps/ydbd
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    cloud/blockstore/tools/nbd
    cloud/blockstore/vhost-server
    cloud/storage/core/tools/testing/qemu/bin
    cloud/storage/core/tools/testing/qemu/image
)

END()
