PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)

SRCS(
    client.proto
    diagnostics.proto
    discovery.proto
    disk.proto
    features.proto
    http_proxy.proto
    logbroker.proto
    notify.proto
    plugin.proto
    rdma.proto
    server.proto
    spdk.proto
    storage.proto
    ydbstats.proto
)

PEERDIR(
    cloud/blockstore/public/api/protos
    cloud/storage/core/config
    cloud/storage/core/protos
)

END()
