PY3_PROGRAM(local_ydb)

PY_SRCS(__main__.py)

PEERDIR(
    yql/essentials/providers/common/proto
    contrib/ydb/public/tools/lib/cmds
)

END()
