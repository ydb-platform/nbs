UNITTEST_FOR(contrib/ydb/public/sdk/cpp/src/client/persqueue_public)

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
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/impl
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/codecs
    contrib/ydb/public/sdk/cpp/src/client/topic/impl
)

YQL_LAST_ABI_VERSION()

SRCS(
    common_ut.cpp
    read_session_ut.cpp
    basic_usage_ut.cpp
    compress_executor_ut.cpp
    compression_ut.cpp
    retry_policy_ut.cpp
)

END()

RECURSE_FOR_TESTS(
    with_offset_ranges_mode_ut
)
