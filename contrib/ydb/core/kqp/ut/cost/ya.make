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
    contrib/ydb/public/lib/ydb_cli/dump/util
    yql/essentials/sql/v1
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
