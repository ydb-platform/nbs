UNITTEST_FOR(contrib/ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_column_stats.cpp
)

END()
