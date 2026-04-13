PY3_PROGRAM(transfer)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/transfer/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

