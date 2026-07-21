PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    test_mon_mtls_auth.py
    test_mon_viewer_access_controls.py
    test_tablets_dev_ui_mon_auth.py
)

SPLIT_FACTOR(20)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/library/flavours/flavours_deps.inc)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/functional/security/lib
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/library/flavours
    contrib/ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
