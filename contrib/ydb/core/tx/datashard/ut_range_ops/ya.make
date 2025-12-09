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
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    yql/essentials/public/udf/service/exception_policy
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/result
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_range_ops.cpp
)

END()
