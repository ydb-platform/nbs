PY3TEST()

FORK_SUBTESTS()
FORK_TEST_FILES()
TIMEOUT(600)
SIZE(MEDIUM)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")

TEST_SRCS(
    conftest.py
    test_auditlog.py
)

DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

REQUIREMENTS(ram:10)

END()
