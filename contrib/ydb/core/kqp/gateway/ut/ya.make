GTEST()

SRCS(
    metadata_conversion.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/gateway
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
    contrib/ydb/services/kesus
    contrib/ydb/services/ydb
    contrib/ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm16
    contrib/ydb/services/metadata
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()

