PY3_PROGRAM(olap_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/public/sdk/python
    library/python/monlib
)

END()
