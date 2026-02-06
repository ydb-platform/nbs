PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_followers.py
    test_compatibility.py
    test_stress.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:all)
REQUIREMENTS(ram:all)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/library/compatibility
)

PEERDIR(
    contrib/python/boto3
    contrib/ydb/tests/library
    contrib/ydb/tests/stress/simple_queue/workload
)

END()