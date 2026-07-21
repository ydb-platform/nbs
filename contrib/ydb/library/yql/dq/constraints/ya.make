LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/providers/common/transform
)

SRCS(
    dq_constraints.cpp
)

YQL_LAST_ABI_VERSION()

END()
