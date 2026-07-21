UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(150)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    indexes_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/local_indexes/bloom
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/kqp/ut/olap/helpers
    contrib/ydb/core/kqp/ut/olap/combinatory
)

YQL_LAST_ABI_VERSION()


END()
