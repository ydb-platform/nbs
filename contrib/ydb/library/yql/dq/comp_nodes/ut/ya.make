UNITTEST_FOR(contrib/ydb/library/yql/dq/comp_nodes)

PEERDIR(
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/comp_nodes/ut/utils
    contrib/ydb/library/yql/dq/comp_nodes/ut/join_perf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)

IF (SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

YQL_LAST_ABI_VERSION()

SRCS(
    dq_hash_combine_ut.cpp
    dq_hash_join_ut.cpp
    dq_rh_hash_ut.cpp
    dq_watermark_generator_ut.cpp
)

END()
