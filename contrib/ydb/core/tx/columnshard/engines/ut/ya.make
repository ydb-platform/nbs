UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
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
    ut_insert_table.cpp
    ut_logs_engine.cpp
    ut_program.cpp
    helper.cpp
)

END()
