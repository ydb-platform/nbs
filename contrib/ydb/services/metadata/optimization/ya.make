LIBRARY()

SRCS(
    abstract.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
