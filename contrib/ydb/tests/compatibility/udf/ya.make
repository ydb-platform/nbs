PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_datetime2.py
    test_digest.py
    test_digest_regression.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:4)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

DEPENDS(
    contrib/ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/compatibility
)

END()
