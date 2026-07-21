PY3_PROGRAM(backup_stress)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/backup/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)