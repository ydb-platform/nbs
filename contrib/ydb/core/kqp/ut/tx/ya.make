UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    kqp_locks_tricky_ut.cpp
    kqp_locks_ut.cpp
    kqp_mvcc_ut.cpp
    kqp_read_committed_ut.cpp
    kqp_sink_locks_ut.cpp
    kqp_sink_mvcc_ut.cpp
    kqp_sink_tx_ut.cpp
    kqp_snapshot_isolation_ut.cpp
    kqp_tx_ut.cpp
    kqp_rollback.cpp
    kqp_online_ro_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
