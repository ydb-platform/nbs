UNITTEST_FOR(contrib/ydb/core/statistics/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/core/statistics/ut_common
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
