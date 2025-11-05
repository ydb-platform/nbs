LIBRARY()

SRCS(
    dq_function_load_meta.cpp
    dq_function_intent_transformer.cpp
    dq_function_provider.cpp
    dq_function_datasource.cpp
    dq_function_datasink.cpp
    dq_function_type_ann.cpp
    dq_function_physical_optimize.cpp
    dq_function_dq_integration.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/common/dq
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/providers/function/expr_nodes
    contrib/ydb/library/yql/providers/function/common
    contrib/ydb/library/yql/providers/function/gateway
    contrib/ydb/library/yql/providers/function/proto
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/opt
)

YQL_LAST_ABI_VERSION()

END()
