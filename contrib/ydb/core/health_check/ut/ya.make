UNITTEST_FOR(contrib/ydb/core/health_check)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/testlib/default
)

SRCS(
    health_check_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
