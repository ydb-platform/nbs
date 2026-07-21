PROTO_LIBRARY()

SRCS(
    gateways_config.proto
    udf_resolver.proto
)

PEERDIR(
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/utils/fetch/proto
    contrib/ydb/library/yql/minikql/runtime_settings/proto
    contrib/ydb/library/yql/providers/common/proto/activation
)

EXCLUDE_TAGS(GO_PROTO)

END()
