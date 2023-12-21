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

ONLY_TAGS(CPP_PROTO)

END()
