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
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

ENV(PQ_OFFSET_RANGES_MODE="1")

SRCDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/ut
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
)

SRCS(
    common_ut.cpp
    read_session_ut.cpp
    basic_usage_ut.cpp
    compress_executor_ut.cpp
    retry_policy_ut.cpp
)

END()
