PROTO_LIBRARY(filestore-private-api-protos)

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/api/protos
    cloud/storage/core/protos

    ydb/core/protos
)

SRCS(
    actions.proto
    tablet.proto
)

END()
