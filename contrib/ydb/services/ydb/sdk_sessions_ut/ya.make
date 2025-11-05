UNITTEST_FOR(contrib/ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SPLIT_FACTOR(60)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(300)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    sdk_sessions_ut.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:14)

END()
