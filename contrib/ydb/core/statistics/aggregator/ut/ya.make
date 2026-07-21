UNITTEST_FOR(contrib/ydb/core/statistics/aggregator)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/udfs/statistics_internal
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/core/statistics/ut_common
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/hyperloglog
)

SRCS(
    ut_analyze_datashard.cpp
    ut_analyze_columnshard.cpp
    ut_analyze_op.cpp
    ut_traverse_datashard.cpp
    ut_traverse_columnshard.cpp
)

END()
