GTEST()

SRCS(
    metadata_conversion.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/gateway
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/services/kesus
    contrib/ydb/services/ydb
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/services/metadata
    contrib/ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

END()

