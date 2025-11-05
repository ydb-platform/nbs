PROTO_LIBRARY()

SRCS(
    fq.proto
)

PEERDIR(
    contrib/ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
