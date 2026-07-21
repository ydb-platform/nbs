UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/services/ydb
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
)

SRCS(
    ut_ru_calculator.cpp
)

YQL_LAST_ABI_VERSION()

END()
