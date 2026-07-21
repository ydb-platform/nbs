LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/dq_integration
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/dq/expr_nodes
)

SRCS(
    dqs_mkql_compiler.cpp
    parser.cpp
)

YQL_LAST_ABI_VERSION()

END()
