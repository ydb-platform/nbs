LIBRARY()

SRCS(
    yql_pg_datasink.cpp
    yql_pg_datasink_execution.cpp
    yql_pg_datasink_type_ann.cpp
    yql_pg_datasource.cpp
    yql_pg_dq_integration.cpp
    yql_pg_datasource_type_ann.cpp
    yql_pg_provider.cpp
    yql_pg_provider.h
    yql_pg_provider_impl.h
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/providers/common/dq
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/providers/pg/expr_nodes
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/utils/log
)

END()
