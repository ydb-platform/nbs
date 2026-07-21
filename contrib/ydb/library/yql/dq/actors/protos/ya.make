PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    dq_events.proto
    dq_stats.proto
    dq_status_codes.proto
)

PEERDIR(
    contrib/ydb/library/actors/protos
    contrib/ydb/public/api/protos
    contrib/ydb/public/api/protos/annotations
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/public/types
)

EXCLUDE_TAGS(GO_PROTO)

END()
