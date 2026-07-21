PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_rolling.py
    test_transfer.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:8)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

DEPENDS(
    contrib/ydb/tests/library/compatibility/binaries
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/compatibility
)

END()
