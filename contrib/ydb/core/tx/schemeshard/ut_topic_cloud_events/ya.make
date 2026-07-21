UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    library/cpp/json
    library/cpp/threading/blocking_queue
    contrib/ydb/core/persqueue/public/cloud_events
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_topic_cloud_events.cpp
)

END()
