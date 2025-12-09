UNITTEST_FOR(contrib/ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    target_discoverer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
