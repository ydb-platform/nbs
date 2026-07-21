PY3_PROGRAM(reconfig_state_storage_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/PyHamcrest/py3
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/reconfig_state_storage_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
