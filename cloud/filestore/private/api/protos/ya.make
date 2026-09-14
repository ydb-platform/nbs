PROTO_LIBRARY(filestore-private-api-protos)

EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/api/protos
    cloud/storage/core/protos
)

SRCS(
    actions.proto
    fastshard.proto
    tablet.proto
)

END()
