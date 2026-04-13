UNITTEST_FOR(contrib/ydb/core/tx/scheme_board)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/tx/tx_allocator
)

SRCS(
    populator_ut.cpp
    ut_helpers.cpp
)

YQL_LAST_ABI_VERSION()

END()
