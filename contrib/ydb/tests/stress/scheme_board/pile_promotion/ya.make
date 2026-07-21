PY3_PROGRAM(pile_promotion_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/PyHamcrest/py3
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/scheme_board/pile_promotion/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
