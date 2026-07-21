PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(45)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    test_split_merge.py
)

PEERDIR(
    contrib/ydb/tests/sql/lib
    contrib/ydb/tests/library
    contrib/ydb/tests/datashard/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()

RECURSE(
    by_load
)
