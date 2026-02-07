PY3TEST()

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
TEST_SRCS(
    test_query_cache.py
)

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

REQUIREMENTS(ram:11)

END()
