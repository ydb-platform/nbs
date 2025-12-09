PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    library/python/monlib
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)


END()
