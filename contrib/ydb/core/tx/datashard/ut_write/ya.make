UNITTEST_FOR(contrib/ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(5)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
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
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/ydb_result
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_write.cpp
)

REQUIREMENTS(ram:32)

END()
