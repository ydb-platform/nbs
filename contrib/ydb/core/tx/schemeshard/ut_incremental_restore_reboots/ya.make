UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_incremental_restore_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
