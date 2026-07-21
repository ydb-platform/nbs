UNITTEST_FOR(contrib/ydb/core/control)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/mind
    contrib/ydb/core/mon
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/persqueue_v1
    contrib/ydb/services/ydb
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
)

SRCS(
    immediate_control_board_actor_ut.cpp
)

END()
