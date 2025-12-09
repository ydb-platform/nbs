PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/ydb/tests/stress/common
)

END()
