PY3TEST()

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(8)

TEST_SRCS(
    test_sql_negative.py
    test_sql_streaming.py
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    contrib/ydb/tests/tools/kqprun
    contrib/ydb/library/yql/tools/astdiff
    contrib/ydb/library/yql/tools/sql2yql
    contrib/ydb/library/yql/tests/common/test_framework/udfs_deps
)

DATA(
    arcadia/contrib/ydb/tests/fq/streaming_optimize/cfg
    arcadia/contrib/ydb/tests/fq/streaming_optimize/suites
)

PEERDIR(
    contrib/ydb/tests/fq/tools
    contrib/ydb/library/yql/tests/common/test_framework
)

END()
