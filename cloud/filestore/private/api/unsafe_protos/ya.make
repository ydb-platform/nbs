PROTO_LIBRARY(filestore-private-api-unsafe-protos)

EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    unsafe.proto
)

PEERDIR(
    cloud/storage/core/protos
)

END()
