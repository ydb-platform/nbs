PY3TEST()

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
TEST_SRCS(
    test_schemeshard_limits.py
)

REQUIREMENTS(
    ram:16
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

FORK_TEST_FILES()
FORK_SUBTESTS()

END()
