UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(100)

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_olap_stats_ut.cpp
    kqp_olap_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/tx/datashard/ut_common
)

YQL_LAST_ABI_VERSION()

IF (SSA_RUNTIME_VERSION)
    CFLAGS(
        -DSSA_RUNTIME_VERSION=$SSA_RUNTIME_VERSION
    )
ENDIF()

END()
