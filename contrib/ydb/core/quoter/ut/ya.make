# Disable test on windows until DEVTOOLS-5591 and DEVTOOLS-5388 will be fixed.
IF (NOT OS_WINDOWS)
    UNITTEST_FOR(contrib/ydb/core/quoter)

    PEERDIR(
        library/cpp/testing/gmock_in_unittest
        contrib/ydb/core/testlib/default
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        kesus_quoter_ut.cpp
        quoter_service_ut.cpp
        ut_helpers.cpp
    )

    # reserve cpu for speed test

    REQUIREMENTS(cpu:4)

    IF (WITH_VALGRIND)
        SIZE(LARGE)
        TIMEOUT(2400)
        TAG(ya:fat)
        SPLIT_FACTOR(20)
    ELSE()
        SIZE(MEDIUM)
        TIMEOUT(600)
    ENDIF()

    END()
ENDIF()
