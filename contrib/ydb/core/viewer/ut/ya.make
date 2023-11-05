UNITTEST_FOR(contrib/ydb/core/viewer)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    viewer_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
)

END()
