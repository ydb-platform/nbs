UNITTEST_FOR(contrib/ydb/public/sdk/cpp/client/ydb_topic)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    contrib/ydb/core/testlib/default
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/impl
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils

    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_topic/include
    contrib/ydb/public/sdk/cpp/client/ydb_topic/common
    contrib/ydb/public/sdk/cpp/client/ydb_topic/impl
    contrib/ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils

    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/persqueue/ut/common
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage_ut.cpp
    describe_topic_ut.cpp
    local_partition_ut.cpp
    topic_to_table_ut.cpp
    trace_ut.cpp
)

RESOURCE(
    contrib/ydb/public/sdk/cpp/client/ydb_topic/ut/resources/topic_A_partition_0_v24-4-2.dat topic_A_partition_0_v24-4-2.dat
    contrib/ydb/public/sdk/cpp/client/ydb_topic/ut/resources/topic_A_partition_1_v24-4-2.dat topic_A_partition_1_v24-4-2.dat
)

END()
