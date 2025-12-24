PY3_PROGRAM(olap_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/olap_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
