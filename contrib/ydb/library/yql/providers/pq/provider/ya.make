LIBRARY()

SRCS(
    yql_pq_datasink.cpp
    yql_pq_datasink_execution.cpp
    yql_pq_datasink_io_discovery.cpp
    yql_pq_datasink_type_ann.cpp
    yql_pq_datasource.cpp
    yql_pq_datasource_constraints.cpp
    yql_pq_datasource_type_ann.cpp
    yql_pq_dq_integration.cpp
    yql_pq_helpers.cpp
    yql_pq_io_discovery.cpp
    yql_pq_load_meta.cpp
    yql_pq_logical_opt.cpp
    yql_pq_mkql_compiler.cpp
    yql_pq_physical_optimize.cpp
    yql_pq_provider.cpp
    yql_pq_provider_impl.cpp
    yql_pq_settings.cpp
    yql_pq_topic_key_parser.cpp
    yql_pq_ytflow_integration.cpp
    yql_pq_ytflow_optimize.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/random_provider
    library/cpp/time_provider

    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/runtime/streaming
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/common/pushdown
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/dq/provider/exec
    contrib/ydb/library/yql/providers/generic/provider
    contrib/ydb/library/yql/providers/pq/cm_client
    contrib/ydb/library/yql/providers/pq/common
    contrib/ydb/library/yql/providers/pq/expr_nodes
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/public/sdk/cpp/src/client/driver

    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/core/dq_integration
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/dq
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/structured_token
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/public/udf

    contrib/ydb/library/yql/providers/ytflow/integration/interface
    contrib/ydb/library/yql/providers/ytflow/integration/proto
    contrib/ydb/library/yql/providers/ytflow/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
