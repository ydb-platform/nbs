UNITTEST_FOR(contrib/ydb/core/public_http)

SIZE(SMALL)

SRCS(
    http_router_ut.cpp
)

PEERDIR(
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
)

YQL_LAST_ABI_VERSION()

END()
