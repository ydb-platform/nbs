LIBRARY()

SRCS(
    yql_ch_read_actor.cpp
    yql_ch_source_factory.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/public/types
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/clickhouse/proto
    contrib/ydb/library/yql/providers/common/http_gateway
)

YQL_LAST_ABI_VERSION()

END()
