UNITTEST_FOR(contrib/ydb/core/tx/balance_coverage)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/testlib/default
)

SRCS(
    balance_coverage_builder_ut.cpp
)

END()
