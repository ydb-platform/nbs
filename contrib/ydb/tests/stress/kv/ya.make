PY3_PROGRAM(workload_kv)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/kv/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)