PY3TEST()

TEST_SRCS(
    conftest.py
    test_serverless.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
TIMEOUT(600)

REQUIREMENTS(
    cpu:4
    ram:32
)
SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/python/tornado/tornado-4
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

END()
