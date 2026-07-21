UNITTEST_FOR(contrib/ydb/library/yql/providers/pure)

SIZE(MEDIUM)

SRCS(
    yql_pure_provider_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/public/result_format
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()
