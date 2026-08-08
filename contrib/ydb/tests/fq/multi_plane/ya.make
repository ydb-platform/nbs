PY3TEST()

FORK_TEST_FILES()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/ydb/tests/tools/datastreams_helpers
    contrib/ydb/tests/tools/fq_runner
)

DEPENDS(
    contrib/ydb/tests/tools/pq_read
)

TEST_SRCS(
    test_cp_ic.py
    test_dispatch.py
    test_retry.py
    test_retry_high_rate.py
)

SIZE(MEDIUM)

END()
