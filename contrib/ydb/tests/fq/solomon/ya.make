PY3TEST()

TEST_SRCS(
    test.py
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

NO_CHECK_IMPORTS()

DEPENDS(
    contrib/ydb/library/yql/udfs/test/test_import
    contrib/ydb/tests/tools/kqprun
)


DATA(
    arcadia/contrib/ydb/library/yql/tests/sql # python files
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

PEERDIR(
    contrib/ydb/tests/fq/tools
    contrib/ydb/library/yql/tests/common/test_framework
)

END()

RECURSE_FOR_TESTS(
    scalar_write
)
