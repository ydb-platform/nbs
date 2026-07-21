PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_bloom_index.py
    test_min_max_index.py
    test_rename_table.py
    test_compression.py
    test_encoding.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:4)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)


DEPENDS(
    contrib/ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/python/boto3
    contrib/ydb/tests/library
    contrib/ydb/tests/library/compatibility
)

END()
