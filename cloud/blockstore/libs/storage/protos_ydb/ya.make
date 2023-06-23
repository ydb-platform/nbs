PROTO_LIBRARY()

SRCS(
    disk.proto
    volume.proto
)

PEERDIR(
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/public/api/protos
    cloud/storage/core/protos
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

END()
