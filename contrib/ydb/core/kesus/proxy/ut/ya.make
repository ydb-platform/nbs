UNITTEST_FOR(contrib/ydb/core/kesus/proxy)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/testlib/default
)

SRCS(
    proxy_actor_ut.cpp
    ut_helpers.cpp
)

YQL_LAST_ABI_VERSION()

END()
