PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/public/sdk/python
    library/python/monlib
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)


END()
