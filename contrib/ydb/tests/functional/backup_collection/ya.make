PY3TEST()

TEST_SRCS(
    "conftest.py"
    "basic_user_scenarios.py"
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/python/pyarrow
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/oss/canonical
    contrib/ydb/tests/oss/ydb_sdk_import
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(30)

END()
