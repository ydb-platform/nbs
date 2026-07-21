UNITTEST_FOR(contrib/ydb/core/tx/columnshard/engines)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/base
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/tx/columnshard/engines/predicate
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/core/arrow_kernels/request
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/core/formats/arrow/program
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/min_max
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/bloom

    contrib/ydb/library/yql/udfs/common/json2
)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    ut_program.cpp
    ut_snapshot_holders.cpp
    ut_scan_snapshot_guard.cpp
    ut_script.cpp
    ut_minmax_index.cpp
    ut_sync_portion_index_reuse.cpp
    ut_predicate_ranges_builder.cpp
    ut_index_info_schema_diff.cpp
    helper.cpp
)

END()