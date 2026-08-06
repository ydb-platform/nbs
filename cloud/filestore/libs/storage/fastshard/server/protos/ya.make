PROTO_LIBRARY()

ONLY_TAGS(CPP_PROTO)

SRCS(
    fastshard.proto
)

PEERDIR(
    cloud/filestore/private/api/protos
    cloud/filestore/public/api/protos
    cloud/storage/core/protos
)

END()
