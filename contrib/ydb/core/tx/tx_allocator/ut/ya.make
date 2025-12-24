UNITTEST_FOR(contrib/ydb/core/tx/tx_allocator)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/mind
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/tx_allocator
)

YQL_LAST_ABI_VERSION()

SRCS(
    txallocator_ut.cpp
    txallocator_ut_helpers.cpp
)

END()
