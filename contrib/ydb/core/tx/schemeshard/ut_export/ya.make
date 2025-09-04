UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(11)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/supp/ubsan_supp.inc)

IF (NOT OS_WINDOWS)
    PEERDIR(
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        contrib/ydb/core/testlib/default
        contrib/ydb/core/tx
        contrib/ydb/core/tx/schemeshard/ut_helpers
        contrib/ydb/core/util
        contrib/ydb/core/wrappers/ut_helpers
        yql/essentials/public/udf/service/exception_policy
        contrib/ydb/core/testlib/audit_helpers
    )
    SRCS(
        ut_export.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
