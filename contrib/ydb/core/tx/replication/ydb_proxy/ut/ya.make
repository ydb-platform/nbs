UNITTEST_FOR(contrib/ydb/core/tx/replication/ydb_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/tx/replication/ut_helpers
    contrib/ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    partition_end_watcher_ut.cpp
    ydb_proxy_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
