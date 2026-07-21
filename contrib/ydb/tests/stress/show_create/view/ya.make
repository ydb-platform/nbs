PY3_PROGRAM(show_create_view)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/show_create/view/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
