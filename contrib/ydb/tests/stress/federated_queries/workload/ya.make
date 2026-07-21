PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/requests
    library/python/port_manager
    contrib/ydb/library/yql/tools/solomon_emulator/client
    contrib/ydb/library/yql/tools/solomon_emulator/lib
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/ydb/tests/stress/common
)

END()
