PY3_PROGRAM(ydb-canondata-sync)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/tools/canondata_sync/lib
)

END()
