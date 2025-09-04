IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
        REQUIREMENTS(ram:12)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    PEERDIR(
        contrib/ydb/core/testlib/default
        contrib/ydb/core/tx
        contrib/ydb/core/tx/schemeshard/ut_helpers
        yql/essentials/public/udf/service/exception_policy
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_sysview_reboots.cpp
    )


END()
ENDIF()