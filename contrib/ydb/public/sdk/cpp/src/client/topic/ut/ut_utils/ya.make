LIBRARY()

SRCS(
    topic_sdk_test_setup.cpp
    topic_sdk_test_setup.h
    txusage_fixture.cpp
    txusage_fixture.h
)

PEERDIR(
    contrib/ydb/library/testlib/common
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/tests/integration/topic/utils

    contrib/ydb/core/base
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/tx/schemeshard/ut_helpers

    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
