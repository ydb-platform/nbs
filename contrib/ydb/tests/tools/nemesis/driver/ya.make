SUBSCRIBER(g:kikimr)
PY3_PROGRAM(nemesis)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/tools/nemesis/library
    contrib/ydb/tools/cfg
)

END()
