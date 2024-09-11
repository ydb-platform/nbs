OWNER(g:cloud-nbs)

PACKAGE()

PEERDIR(
    ydb/apps/ydbd
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    cloud/blockstore/tools/nbd
)

END()
