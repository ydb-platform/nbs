UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

SIZE(SMALL)

SRCS(
    kqp_tli_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
