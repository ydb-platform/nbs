UNITTEST()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    contrib/ydb/core/testlib/default
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/include
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

    contrib/ydb/public/sdk/cpp/src/client/topic/codecs
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/topic/impl

    contrib/ydb/public/sdk/cpp/src/client/federated_topic
    contrib/ydb/public/sdk/cpp/src/client/federated_topic/impl

    contrib/ydb/public/sdk/cpp/tests/integration/topic/utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage_ut.cpp
)

END()
