UNITTEST_FOR(contrib/ydb/services/persqueue_v1)

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
    ic_cache_ut.cpp
    describe_topic_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/client/server
    contrib/ydb/services/persqueue_v1
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/api/grpc
)

YQL_LAST_ABI_VERSION()

END()
