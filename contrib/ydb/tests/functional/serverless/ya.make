PY3TEST()

TEST_SRCS(
    conftest.py
    test_serverless.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

SIZE(MEDIUM)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
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
