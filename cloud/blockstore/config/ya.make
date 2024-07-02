PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
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
    server.proto
    spdk.proto
    storage.proto
    ydbstats.proto
)

PEERDIR(
    cloud/blockstore/public/api/protos
    cloud/storage/core/config
    cloud/storage/core/protos
    contrib/ydb/core/protos
)

END()
