PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/requests/py3
    contrib/ydb/tests/library
)

END()
