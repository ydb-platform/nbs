UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_cost_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/udfs/common/knn
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
