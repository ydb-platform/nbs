UNITTEST_FOR(contrib/ydb/core/tx/replication/ydb_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    contrib/ydb/core/tx/replication/ut_helpers
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    library/cpp/testing/unittest
)

SRCS(
    ydb_proxy_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
