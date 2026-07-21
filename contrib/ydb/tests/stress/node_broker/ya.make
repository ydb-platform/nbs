PY3_PROGRAM(node_broker)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/node_broker/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
