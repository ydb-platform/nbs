PY3_PROGRAM(workload_topic)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/topic/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
