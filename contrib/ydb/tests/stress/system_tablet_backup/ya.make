PY3_PROGRAM(system_tablet_backup)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/system_tablet_backup/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
