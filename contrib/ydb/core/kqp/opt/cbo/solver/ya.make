LIBRARY()

PEERDIR(
    contrib/ydb/core/kqp/expr_nodes
    contrib/ydb/core/kqp/opt/cbo
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/opt/core
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/dq_integration
    contrib/ydb/library/yql/core/expr_nodes_gen
)

SRCS(
    kqp_opt_cbo_latency_predictor.cpp
    kqp_opt_conflict_rules_collector.cpp
    kqp_opt_join.cpp
    kqp_opt_join_cbo_factory.cpp
    kqp_opt_join_cost_based.cpp
    kqp_opt_join_tree_node.cpp
    kqp_opt_predicate_selectivity.cpp
    kqp_opt_stat.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
