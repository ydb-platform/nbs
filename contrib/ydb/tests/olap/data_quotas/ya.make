PY3TEST()
FORK_SUBTESTS()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

TEST_SRCS(
    test_quota_exhaustion.py
)

REQUIREMENTS(cpu:4)

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
contrib/ydb/tests/library
contrib/ydb/tests/library/test_meta
)

END()
