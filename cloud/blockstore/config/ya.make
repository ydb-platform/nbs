PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    cells.proto
    client.proto
    diagnostics.proto
    discovery.proto
    disk.proto
    grpc_client.proto
    http_proxy.proto
    logbroker.proto
    notify.proto
    plugin.proto
    rdma.proto
    root_kms.proto
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
