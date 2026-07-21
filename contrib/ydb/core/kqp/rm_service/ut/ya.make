UNITTEST_FOR(contrib/ydb/core/kqp/rm_service)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_rm_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
