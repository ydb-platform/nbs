PY3TEST()

TEST_SRCS(
    test_generator.py
    test_init.py
)

SIZE(MEDIUM)

IF(NOT SANITIZER_TYPE)
    REQUIREMENTS(ram:8)
ELSE()
    REQUIREMENTS(ram:16)
ENDIF()

ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

DEPENDS(
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
    contrib/python/PyHamcrest
    contrib/ydb/tests/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()
END()
