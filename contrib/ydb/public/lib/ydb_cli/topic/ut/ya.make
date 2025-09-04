UNITTEST_FOR(contrib/ydb/public/lib/ydb_cli/topic)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    topic_write.h
    topic_write.cpp
    topic_write_ut.cpp
    topic_read_ut.cpp
)

PEERDIR(
    library/cpp/histogram/hdr
    library/cpp/threading/local_executor
    contrib/ydb/core/fq/libs/private_client
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/lib/experimental
    contrib/ydb/public/lib/ydb_cli/commands
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
