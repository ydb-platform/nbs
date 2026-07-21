PY3_PROGRAM(workload_vector)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/vector_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
