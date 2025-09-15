PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    client.proto
    diagnostics.proto
    filesystem.proto
    http_proxy.proto
    nfs_gateway.proto
    server.proto
    spdk.proto
    storage.proto
    vfs.proto
    vhost.proto
)

PEERDIR(
    cloud/filestore/public/api/protos
    cloud/storage/core/config
    cloud/storage/core/protos
)

END()
