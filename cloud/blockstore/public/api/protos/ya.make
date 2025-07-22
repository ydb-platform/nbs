PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    actions.proto
    checkpoints.proto
    client.proto
    cms.proto
    discovery.proto
    disk.proto
    encryption.proto
    endpoints.proto
    headers.proto
    io.proto
    local_ssd.proto
    metrics.proto
    mount.proto
    ping.proto
    placement.proto
    rdma.proto
    throttling.proto
    volume.proto
)

PEERDIR(
    cloud/storage/core/protos

    library/cpp/lwtrace/protos
)

END()
