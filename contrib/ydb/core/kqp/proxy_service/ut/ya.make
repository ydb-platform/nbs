UNITTEST_FOR(contrib/ydb/core/kqp/proxy_service)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_proxy_ut.cpp
    kqp_script_executions_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/run_script_actor
    contrib/ydb/core/kqp/proxy_service
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/kqp/workload_service/ut/common
    yql/essentials/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
