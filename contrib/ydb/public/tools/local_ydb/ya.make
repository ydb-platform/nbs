PY3_PROGRAM(local_ydb)

PY_SRCS(__main__.py)

PEERDIR(
    contrib/ydb/public/tools/lib/cmds
)

END()
