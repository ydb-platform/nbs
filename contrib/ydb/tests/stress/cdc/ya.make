PY3_PROGRAM(cdc)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/cdc/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
