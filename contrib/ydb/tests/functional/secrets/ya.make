RECURSE(
    lib
    slow
)

PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    test_old_secrets_usage.py
    test_secrets.py
    test_secrets_usage.py
    test_secrets_monitoring.py
)

SPLIT_FACTOR(20)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/library/flavours/flavours_deps.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/s3_recipe/recipe.inc)

DEPENDS(
)

PEERDIR(
    contrib/python/boto3
    contrib/ydb/tests/functional/secrets/lib
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/library/flavours
    contrib/ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
