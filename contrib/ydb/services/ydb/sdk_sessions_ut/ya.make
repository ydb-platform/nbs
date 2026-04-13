UNITTEST_FOR(contrib/ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SPLIT_FACTOR(60)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    sdk_sessions_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/lib/ut_helpers
)

YQL_LAST_ABI_VERSION()

END()
