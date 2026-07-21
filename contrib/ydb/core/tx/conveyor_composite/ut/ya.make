UNITTEST_FOR(contrib/ydb/core/tx/conveyor_composite/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

REQUIREMENTS(cpu:4)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/core/arrow_kernels/request
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/hooks/testing

    contrib/ydb/library/yql/udfs/common/json2
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_simple.cpp
)

END()
