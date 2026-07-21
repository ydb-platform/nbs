LIBRARY()

SRCS(
    yql_dq_integration.cpp
    yql_dq_optimization.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    transform
)
