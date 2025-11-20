UNITTEST_FOR(contrib/ydb/core/http_proxy)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/resource
    contrib/ydb/core/base
    contrib/ydb/core/http_proxy
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/http
    contrib/ydb/library/grpc/server
    contrib/ydb/library/grpc/server/actors
    contrib/ydb/library/persqueue/tests
    contrib/ydb/library/testlib/service_mocks
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/client/ydb_discovery
    contrib/ydb/public/sdk/cpp/client/ydb_types
    contrib/ydb/services/datastreams
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/ydb
    contrib/ydb/services/ymq
)

SRCS(
    json_proto_conversion_ut.cpp
    datastreams_fixture.h
)

RESOURCE(
    internal_counters.json internal_counters.json
    proxy_counters.json proxy_counters.json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    inside_ydb_ut
)
