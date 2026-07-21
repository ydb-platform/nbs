PY3_PROGRAM(workload_mixed)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/mixedpy/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)