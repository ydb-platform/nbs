UNITTEST_FOR(contrib/ydb/core/kqp/federated_query)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/public/api/protos
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
)

SRCS(
    kqp_federated_query_helpers_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
