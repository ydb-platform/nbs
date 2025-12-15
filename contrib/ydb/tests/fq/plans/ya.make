PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/python/boto3
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    contrib/ydb/tests/tools/fq_runner
)

DEPENDS(
    contrib/python/moto/bin
)

TEST_SRCS(
    test_stats_mode.py
)

PY_SRCS(
    conftest.py
)

SIZE(MEDIUM)

END()
