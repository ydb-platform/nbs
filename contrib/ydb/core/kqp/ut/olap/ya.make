UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(150)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    GLOBAL blobs_sharing_ut.cpp
    GLOBAL kqp_olap_ut.cpp
    aggregations_ut.cpp
    clickbench_ut.cpp
    locks_ut.cpp
    optimizer_ut.cpp
    peephole_ut.cpp
    sys_view_ut.cpp
    tiering_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/protos
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/kqp/ut/olap/helpers
    contrib/ydb/core/kqp/ut/olap/combinatory
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/library/aws_init
    contrib/ydb/public/sdk/cpp/src/client/operation
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    indexes
    types
    operations
    statistics
    storage
    reading
    pushdown
)
