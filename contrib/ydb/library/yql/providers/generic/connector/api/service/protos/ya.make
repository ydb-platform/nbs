PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/public/api/protos
)

# Because Go is excluded in YDB protofiles
EXCLUDE_TAGS(GO_PROTO)

SRCS(
    connector.proto
    error.proto
)

END()
