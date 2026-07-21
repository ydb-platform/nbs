PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_TEST_PATH="contrib/ydb/tests/stress/s3_backups/s3_backups")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/tests/stress/s3_backups
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/s3_backups/workload
)

END()
