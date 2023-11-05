UNITTEST_FOR(contrib/ydb/core/http_proxy)

SIZE(SMALL)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/resource
    contrib/ydb/core/http_proxy
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/client/ydb_types
    contrib/ydb/services/kesus
    contrib/ydb/services/datastreams
    contrib/ydb/services/persqueue_cluster_discovery
)

SRCS(
    json_proto_conversion_ut.cpp
)

END()
