PY3_PROGRAM(ctas)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/ctas/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
