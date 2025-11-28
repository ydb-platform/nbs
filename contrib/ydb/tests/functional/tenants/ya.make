PY3TEST()

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")

TEST_SRCS(
    conftest.py
    test_db_counters.py
    test_dynamic_tenants.py
    test_tenants.py
    test_storage_config.py
    test_system_views.py
    test_publish_into_schemeboard_with_common_ssring.py
)

SPLIT_FACTOR(20)
TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/python/requests
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

FORK_SUBTESTS()

REQUIREMENTS(ram:10)

END()
