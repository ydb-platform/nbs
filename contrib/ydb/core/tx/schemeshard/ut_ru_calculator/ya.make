UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/tx/tx_proxy
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    contrib/ydb/services/ydb

    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm16

)

SRCS(
    ut_ru_calculator.cpp
)

YQL_LAST_ABI_VERSION()

END()
