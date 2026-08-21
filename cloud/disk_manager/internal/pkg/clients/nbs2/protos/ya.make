PROTO_LIBRARY()

GRPC()
ONLY_TAGS(GO_PROTO)

SRCS(
    ydb_nbs.proto
    ydb_nbs_v1.proto
)

END()
