PY3TEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=false)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner.inc)

PEERDIR(
    contrib/python/boto3
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    contrib/ydb/tests/tools/fq_runner
)

DEPENDS(
    contrib/python/moto/bin
)

TEST_SRCS(
    test_insert_restarts.py
)

PY_SRCS(
    conftest.py
)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
