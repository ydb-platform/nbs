PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_scalar_topic_write.py
    test_streaming.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:4)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

DEPENDS(
    contrib/ydb/tests/library/compatibility/binaries
    contrib/ydb/tests/tools/pq_read
)

PEERDIR(
    contrib/python/boto3
    contrib/ydb/tests/library
    contrib/ydb/tests/library/compatibility
    contrib/ydb/tests/library/test_meta
    contrib/ydb/tests/tools/datastreams_helpers
)

END()
