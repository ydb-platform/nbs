UNITTEST_FOR(contrib/ydb/public/sdk/cpp/src/client/topic)

REQUIREMENTS(ram:32)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
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
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/impl
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/topic/common
    contrib/ydb/public/sdk/cpp/src/client/topic/impl
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/library/issue

    contrib/ydb/public/sdk/cpp/tests/integration/topic/utils

    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage_ut.cpp
    describe_topic_ut.cpp
    local_partition_ut.cpp
    topic_to_table_ut.cpp
)

RESOURCE(
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/resources/topic_A_partition_0_v24-4-2.dat topic_A_partition_0_v24-4-2.dat
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/resources/topic_A_partition_1_v24-4-2.dat topic_A_partition_1_v24-4-2.dat
)

END()

RECURSE_FOR_TESTS(
    with_direct_read_ut
)
