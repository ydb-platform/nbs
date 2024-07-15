PY3TEST()

FORK_TEST_FILES()
TIMEOUT(600)
SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_session_pool.py
    test_crud.py
    test_indexes.py
    test_discovery.py
    test_execute_scheme.py
    test_insert.py
    test_isolation.py
    test_public_api.py
    test_read_table.py
    test_session_grace_shutdown.py
)

DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/python/requests
    contrib/python/tornado/tornado-4
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

REQUIREMENTS(ram:10)

END()
