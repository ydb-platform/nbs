UNITTEST_FOR(contrib/ydb/core/kqp/executer_actor)

SIZE(MEDIUM)

SRCS(
    kqp_executer_ut.cpp
    # kqp_tasks_graph_ut.cpp
    max_tasks_graph_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
