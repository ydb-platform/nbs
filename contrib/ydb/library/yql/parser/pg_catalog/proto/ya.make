PROTO_LIBRARY()

SRCS(
    pg_catalog.proto
)

PEERDIR(
    contrib/ydb/library/yql/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
