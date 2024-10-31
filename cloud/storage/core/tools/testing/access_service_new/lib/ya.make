PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    cloud/storage/core/tools/common/python
    contrib/python/requests/py3
    contrib/ydb/tests/library
    cloud/blockstore/pylibs/ydb/tests/library
)

END()
