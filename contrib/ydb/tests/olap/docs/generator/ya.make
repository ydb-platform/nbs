PY3_PROGRAM()

PY_SRCS(
    parser.py
    __main__.py
)

PEERDIR(
    contrib/ydb/tests/olap/scenario/helpers
    contrib/python/PyYAML
)

END()
