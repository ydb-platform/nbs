PROTO_LIBRARY()

SRCS(
    runtime_settings.proto
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/proto/activation
)

EXCLUDE_TAGS(GO_PROTO)

END()
