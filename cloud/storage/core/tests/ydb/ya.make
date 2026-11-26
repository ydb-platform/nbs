PY3_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/public/api/protos
    contrib/ydb/tests/library

    library/python/testing/yatest_common
)

END()
