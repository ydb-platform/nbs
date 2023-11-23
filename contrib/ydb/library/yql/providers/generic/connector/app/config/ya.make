PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    client.proto
    server.proto
)

PEERDIR(
    contrib/ydb/library/yql/providers/generic/connector/api/common
)

END()
