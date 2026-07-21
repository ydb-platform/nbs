PY3_LIBRARY()

PY_SRCS(
    bloom_filter_index.py
    fulltext_index.py
    vector_index.py
    json_index.py
    insert_delete_all_types.py
    select_partition.py
    secondary_index.py
    tli.py
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/library/fixtures
    contrib/ydb/public/sdk/python
    contrib/ydb/tests/datashard/lib
)

END()
