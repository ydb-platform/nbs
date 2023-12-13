PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/requests/py3
    contrib/python/retrying
    devtools/ya/core/config
    library/python/testing/recipe
    contrib/ydb/tests/library
)

END()
