UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(200)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

IF(SANITIZER_TYPE == "memory")
    # Increase MSan memory limit due to YQL-19940.
    # Just double default memory requirements since we run MSan without origin tracking by default.
    REQUIREMENTS(
        ram:16
    )
ENDIF()

SRCS(
    kqp_block_hash_join_ut.cpp
    kqp_join_order_ut.cpp
    kqp_benches_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/opt/cbo/bench
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/udfs/common/digest
)

DATA (
    arcadia/contrib/ydb/core/kqp/ut/join
    arcadia/contrib/ydb/library/benchmarks/queries
    arcadia/contrib/ydb/library/benchmarks/gen_queries/consts.yql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    index_lookup
)
