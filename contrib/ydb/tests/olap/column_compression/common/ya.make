PY3_LIBRARY()

    PY_SRCS (
        base.py
    )

    PEERDIR(
        contrib/ydb/tests/library
        contrib/ydb/public/sdk/python
        contrib/ydb/public/sdk/python/enable_v3_new_behavior
        contrib/ydb/tests/olap/scenario/helpers
        contrib/ydb/tests/olap/common
    )

END()
