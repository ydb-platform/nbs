PY3_PROGRAM(tstool)

OWNER(alexvru)

PY_MAIN(tstool)

PY_SRCS(
    TOP_LEVEL
    tstool.py
)

PEERDIR(
    contrib/ydb/core/protos
)

END()
