PY3_LIBRARY()

PY_SRCS(
    __init__.py
)
BUNDLE(
    contrib/ydb/apps/ydb NAME ydb_cli
)
RESOURCE(ydb_cli ydb_cli)
PEERDIR(
    contrib/ydb/tests/stress/common

    library/python/monlib
    library/python/resource
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/ydb/tests/library
)

END()
