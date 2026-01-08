UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

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

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    ut_program.cpp
    ut_script.cpp
    helper.cpp
)

END()
