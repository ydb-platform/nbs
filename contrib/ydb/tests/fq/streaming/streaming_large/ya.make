PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)


TEST_SRCS(
    test_cluster_restarts.py
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
TAG(ya:fat)


PY_SRCS(
    conftest.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    library/recipes/common
    contrib/ydb/tests/olap/common
    contrib/ydb/tests/tools/datastreams_helpers
    contrib/ydb/tests/fq/streaming_common
)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/tools/pq_read
    contrib/ydb/library/yql/udfs/common/python/python3_small
)

END()
