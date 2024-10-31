PY3_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(
    contrib/ydb/tests/library
    cloud/blockstore/pylibs/ydb/tests/library
)

END()
