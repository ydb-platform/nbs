PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(32)

TEST_SRCS(
    test_stress.py
)

SIZE(LARGE)
REQUIREMENTS(ram:32 cpu:96)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

DEPENDS(
    contrib/ydb/tests/library/compatibility/binaries
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/datashard/lib
    contrib/ydb/tests/stress/simple_queue/workload
    contrib/ydb/tests/library/compatibility
)

END()
