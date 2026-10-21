UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/dump/util
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib
    contrib/ydb/core/tx
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/public/sdk/cpp/src/client/types
)

YQL_LAST_ABI_VERSION()

SRCS(
    kqp_read_null_ut.cpp
)


END()
