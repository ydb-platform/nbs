LIBRARY()

SRCS(
    yql_configuration_transformer.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/providers/common/config
)

YQL_LAST_ABI_VERSION()

END()
