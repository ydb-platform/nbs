PROTO_LIBRARY()

SRCS(
    protocol.proto
)

PEERDIR(
    cloud/blockstore/libs/storage/protos
)

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

END()
