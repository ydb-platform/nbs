UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    kqp_batch_update_ut.cpp
    kqp_batch_delete_ut.cpp
    kqp_batch_pea_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
