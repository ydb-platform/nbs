UNITTEST_FOR(contrib/ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    contrib/ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    assign_tx_id_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
