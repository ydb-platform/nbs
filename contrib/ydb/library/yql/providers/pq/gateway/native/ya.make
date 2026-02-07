LIBRARY()

SRCS(
    yql_pq_gateway.cpp
    yql_pq_session.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/metrics
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/pq/cm_client
    contrib/ydb/library/yql/providers/pq/provider
    contrib/ydb/library/yql/utils
    contrib/ydb/public/sdk/cpp/client/ydb_datastreams
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_topic
)

END()
