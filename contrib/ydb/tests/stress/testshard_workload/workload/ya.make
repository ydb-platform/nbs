PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

BUNDLE(
    contrib/ydb/apps/ydb NAME ydb_cli
)
RESOURCE(ydb_cli ydb_cli)

BUNDLE(
    contrib/ydb/tools/tsserver NAME tsserver
)
RESOURCE(tsserver tsserver)
PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/public/sdk/python
    library/python/resource
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)

END()
