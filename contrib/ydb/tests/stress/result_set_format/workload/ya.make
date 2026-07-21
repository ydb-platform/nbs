PY3_LIBRARY()

PY_SRCS(
    __init__.py
    common.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/ydb/tests/stress/result_set_format/workload/type
)

END()
