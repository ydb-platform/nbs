UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/public/sdk/cpp/client/ydb_types
)

YQL_LAST_ABI_VERSION()

SRCS(
    kqp_read_null_ut.cpp
)


END()
