PY2TEST()

TAG(ya:manual)

TEST_SRCS(
    test.py
)

SIZE(MEDIUM)

NO_CHECK_IMPORTS()

DEPENDS(
    contrib/ydb/library/yql/tools/astdiff
    contrib/ydb/library/yql/tools/dqrun
    contrib/ydb/library/yql/udfs/test/test_import
)


DATA(
    arcadia/contrib/ydb/library/yql/tests/sql # python files
    arcadia/contrib/ydb/library/yql/mount
    arcadia/contrib/ydb/library/yql/cfg/tests
    arcadia/contrib/ydb/library/yql/tests/sql
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

PEERDIR(
    library/python/testing/swag/lib
    contrib/ydb/library/yql/tests/common/test_framework
)

END()
