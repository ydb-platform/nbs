PY3_LIBRARY()

PY_SRCS(
    __init__.py
    fulltext.py
    json.py
    safe_parametrize.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/public/sdk/python
)

END()
