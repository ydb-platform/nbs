PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    balancer.proto
    blob.proto
    checkpoints.proto
    configs.proto
    disk.proto
    tablet.proto
    volume.proto
)

PEERDIR(
    cloud/blockstore/public/api/protos
    cloud/storage/core/protos
)

END()
