PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
TEST_SRCS(
    test_schemeshard_limits.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

FORK_TEST_FILES()
FORK_SUBTESTS()

END()
