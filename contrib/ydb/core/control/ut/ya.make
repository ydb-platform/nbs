UNITTEST_FOR(contrib/ydb/core/control)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    library/cpp/testing/unittest
    util
    contrib/ydb/core/base
    contrib/ydb/core/mind
    contrib/ydb/core/mon
    yql/essentials/sql/pg_dummy
    contrib/ydb/services/ydb
    contrib/ydb/services/persqueue_v1
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
)

SRCS(
    immediate_control_board_actor_ut.cpp
)

END()
