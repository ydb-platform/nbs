PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    conftest.py
    test_create_users.py
    test_create_users_strict_acl_checks.py
    test_db_counters.py
    test_dynamic_tenants.py
    test_tenants.py
    test_storage_config.py
    test_system_views.py
    test_auth_system_views.py
    test_publish_into_schemeboard_with_common_ssring.py
    test_user_administration.py
    test_users_groups_with_acl.py
)

SPLIT_FACTOR(20)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/library/flavours/flavours_deps.inc)

DEPENDS(
)

PEERDIR(
    contrib/python/requests
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/library/flavours
    contrib/ydb/tests/library/clients
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:1)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
