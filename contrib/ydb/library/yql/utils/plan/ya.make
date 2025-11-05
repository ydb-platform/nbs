LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/expr_nodes
)

SRCS(
    plan_utils.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
