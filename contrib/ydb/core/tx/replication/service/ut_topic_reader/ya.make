UNITTEST_FOR(contrib/ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    contrib/ydb/core/tx/replication/ut_helpers
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    library/cpp/testing/unittest
)

SRCS(
    topic_reader_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
