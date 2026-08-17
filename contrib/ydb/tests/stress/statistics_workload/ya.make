PY3_PROGRAM(statistics_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/public/sdk/python
    library/python/monlib
)

END()
