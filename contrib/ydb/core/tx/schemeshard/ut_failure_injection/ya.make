UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

SRCS(
    ut_failure_injection.cpp
)

YQL_LAST_ABI_VERSION()

END()
