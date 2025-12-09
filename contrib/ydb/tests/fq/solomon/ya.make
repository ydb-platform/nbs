PY3TEST()

TEST_SRCS(
    test.py
)

SIZE(MEDIUM)

NO_CHECK_IMPORTS()

DEPENDS(
    yql/essentials/udfs/test/test_import
    contrib/ydb/tests/tools/kqprun
)


DATA(
    arcadia/contrib/ydb/library/yql/tests/sql # python files
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator_grpc/recipe.inc)

PEERDIR(
    contrib/ydb/tests/fq/tools
    yql/essentials/tests/common/test_framework
)

END()