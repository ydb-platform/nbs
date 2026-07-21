PY3_LIBRARY()

PY_SRCS(
    compression.py
    data_types.py
    mixed.py
    schema_inclusion.py
)

PEERDIR(
    contrib/ydb/tests/datashard/lib
    contrib/ydb/tests/stress/common
    contrib/ydb/public/sdk/python
    contrib/python/pyarrow
)

END()
