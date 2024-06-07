PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/ydb/tests/tools/datastreams_helpers
)

DEPENDS(contrib/ydb/tests/tools/pq_read)

TEST_SRCS(
    test_commit.py
    test_timeout.py
)

END()
