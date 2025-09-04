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
    contrib/libs/double-conversion
    library/cpp/string_utils/quote
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/util
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/core/ydb_convert
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    contrib/ydb/core/testlib/audit_helpers
)

SRCS(
    ut_restore.cpp
)

YQL_LAST_ABI_VERSION()

END()
