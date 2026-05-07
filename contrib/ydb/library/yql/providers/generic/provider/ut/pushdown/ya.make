UNITTEST_FOR(contrib/ydb/library/yql/providers/generic/provider)

SRCS(
    pushdown_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/random_provider
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/services
    contrib/ydb/library/yql/dq/expr_nodes
    yql/essentials/minikql
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/generic/expr_nodes
    yql/essentials/providers/result/provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/sql/pg_dummy
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
