UNITTEST_FOR(contrib/ydb/core/tx/conveyor_composite/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)
SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/base
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/counters
    yql/essentials/sql/pg_dummy
    yql/essentials/core/arrow_kernels/request
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/hooks/testing

    yql/essentials/udfs/common/json2
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_simple.cpp
)

END()
