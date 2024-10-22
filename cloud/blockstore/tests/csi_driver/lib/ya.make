PY3_LIBRARY()

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
    cloud/storage/core/protos
    contrib/ydb/core/protos
    contrib/ydb/tests/library
)

PY_SRCS(
    __init__.py
    csi_runner.py
)

END()
