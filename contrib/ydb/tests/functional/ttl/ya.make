PY3TEST()

TEST_SRCS(
    test_ttl.py
)

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
    contrib/python/PyHamcrest
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
