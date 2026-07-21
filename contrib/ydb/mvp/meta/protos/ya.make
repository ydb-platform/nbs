PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    config.proto
)

PEERDIR(
    contrib/ydb/mvp/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
