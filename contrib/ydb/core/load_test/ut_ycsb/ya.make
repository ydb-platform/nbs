UNITTEST_FOR(contrib/ydb/core/load_test)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/load_test
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    yql/essentials/public/udf/service/exception_policy
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/result
)

YQL_LAST_ABI_VERSION()

SRCS(
    ../ut_ycsb.cpp
)

END()
