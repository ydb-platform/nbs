PY3_LIBRARY()

PY_SRCS(
    __init__.py
    secrets_plugin.py
)

PEERDIR(
    contrib/python/pytest
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/library/flavours
    contrib/ydb/tests/oss/ydb_sdk_import
)

END()
