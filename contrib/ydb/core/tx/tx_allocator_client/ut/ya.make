UNITTEST_FOR(contrib/ydb/core/tx/tx_allocator_client)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/mind
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/tx_allocator_client
)

YQL_LAST_ABI_VERSION()

SRCS(
    actor_client_ut.cpp
    ut_helpers.cpp
)

END()
