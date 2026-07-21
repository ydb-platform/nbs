UNITTEST_FOR(contrib/ydb/core/persqueue/public/cloud_events)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/audit
    contrib/ydb/core/persqueue/public/cloud_events
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/protos
    library/cpp/json
    library/cpp/testing/unittest
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

SRCS(
    cloud_events_ut.cpp
)

END()
