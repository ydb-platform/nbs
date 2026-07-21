UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(60)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

IF(SANITIZER_TYPE == "memory")
    # Increase MSan memory limit due to YQL-19940.
    # Just double default memory requirements since we run MSan without origin tracking by default.
    REQUIREMENTS(
        ram:16
    )
ENDIF()

SRCS(
    kqp_complex_join_query_ut.cpp
    kqp_flip_join_ut.cpp
    kqp_index_lookup_join_ut.cpp
    kqp_join_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/udfs/common/digest
)

YQL_LAST_ABI_VERSION()

END()
