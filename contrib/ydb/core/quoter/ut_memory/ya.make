IF (NOT OS_WINDOWS)
    UNITTEST_FOR(contrib/ydb/core/quoter)

    TAG(ya:manual)

    PEERDIR(
        library/cpp/testing/gmock_in_unittest
        contrib/ydb/core/testlib/default
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_helpers.cpp
        ut_memory/quoter_memory_ut.cpp
    )

    SIZE(LARGE)
    TIMEOUT(600)

    END()
ENDIF()
