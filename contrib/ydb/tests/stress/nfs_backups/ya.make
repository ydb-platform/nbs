PY3_PROGRAM(nfs_backups)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/nfs_backups/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
