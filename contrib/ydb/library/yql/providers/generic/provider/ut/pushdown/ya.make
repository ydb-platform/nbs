UNITTEST_FOR(contrib/ydb/library/yql/providers/generic/provider)

TAG(ya:manual)

SRCS(
    pushdown_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/random_provider
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/generic/expr_nodes
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/ydb/library/yql/sql/pg_dummy
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
