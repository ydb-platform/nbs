PY3_LIBRARY()

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
    cloud/filestore/tests/python/lib
    cloud/storage/core/protos
    ydb/core/protos
    ydb/tests/library
)

PY_SRCS(
    __init__.py
    csi_runner.py
)

END()
