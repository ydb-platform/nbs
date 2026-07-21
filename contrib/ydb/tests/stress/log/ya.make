PY3_PROGRAM(workload_log)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/log/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)