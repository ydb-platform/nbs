PROTO_LIBRARY()

SRCS(
    access_provider.proto
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
