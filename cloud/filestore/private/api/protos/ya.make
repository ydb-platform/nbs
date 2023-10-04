PROTO_LIBRARY(filestore-private-api-protos)

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    cloud/filestore/public/api/protos
    cloud/storage/core/protos
)

SRCS(
    tablet.proto
)

END()
