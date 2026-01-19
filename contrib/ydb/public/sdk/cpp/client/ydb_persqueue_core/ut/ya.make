UNITTEST()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
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
    contrib/ydb/public/sdk/cpp/client/ydb_topic/codecs
    contrib/ydb/public/sdk/cpp/client/ydb_topic/impl
)

YQL_LAST_ABI_VERSION()

SRCDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/ut
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
)

SRCS(
    common_ut.cpp
    read_session_ut.cpp
    basic_usage_ut.cpp
    compress_executor_ut.cpp
    compression_ut.cpp
    retry_policy_ut.cpp
)

END()
