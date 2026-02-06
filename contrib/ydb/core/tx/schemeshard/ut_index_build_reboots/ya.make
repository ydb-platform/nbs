UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SPLIT_FACTOR(40)
ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/public/udf/service/exception_policy
    contrib/ydb/public/sdk/cpp/src/client/table
)

SRCS(
    ut_index_build_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
