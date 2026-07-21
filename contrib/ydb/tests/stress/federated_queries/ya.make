PY3_PROGRAM(federated_queries)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/federated_queries/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
