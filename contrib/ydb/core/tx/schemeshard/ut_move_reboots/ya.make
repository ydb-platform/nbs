IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)


    PEERDIR(
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        contrib/ydb/core/testlib/default
        contrib/ydb/core/tx
        contrib/ydb/core/tx/schemeshard/ut_helpers
        yql/essentials/public/udf/service/exception_policy
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_move_reboots.cpp
    )

    END()
ENDIF()
