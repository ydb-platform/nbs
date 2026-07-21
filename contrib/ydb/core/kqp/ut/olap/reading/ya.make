UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(150)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    simple_reader_ut.cpp
    sparsed_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/kqp/ut/olap/helpers
    contrib/ydb/core/kqp/ut/olap/combinatory
    contrib/ydb/core/wrappers
)

YQL_LAST_ABI_VERSION()

END()
