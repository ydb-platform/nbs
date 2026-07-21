UNITTEST_FOR(contrib/ydb/core/persqueue/public/schema)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    create_topic_ut.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/public
    contrib/ydb/core/testlib/grpc_request
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    library/cpp/testing/unittest
)

ENV(INSIDE_YDB="1")

END()
