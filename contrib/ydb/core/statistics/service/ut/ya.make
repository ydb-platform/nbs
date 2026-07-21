UNITTEST_FOR(contrib/ydb/core/statistics/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

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
    ut_basic_statistics.cpp
    ut_column_statistics.cpp
    ut_http_request.cpp
)

END()

RECURSE_FOR_TESTS(
    ut_aggregation
)
