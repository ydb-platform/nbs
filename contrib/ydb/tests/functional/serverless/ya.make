PY3TEST()

TEST_SRCS(
    conftest.py
    test_serverless.py
)

SPLIT_FACTOR(50)
FORK_TEST_FILES()
FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
DEPENDS(
)

PEERDIR(
    contrib/python/tornado/tornado-4
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

END()
