PROTO_LIBRARY()

PEERDIR(
    library/cpp/monlib/encode/legacy_protobuf/protos
)

SRCS(
    mon_proto.proto
)

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

END()
