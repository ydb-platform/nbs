PY3_PROGRAM(simple_queue)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/simple_queue/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

