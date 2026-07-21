PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    test_secrets_logging.py
    test_secrets_monitoring.py
)

TAG(ya:fat)
SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/library/flavours/flavours_deps.inc)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/functional/secrets/lib
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/library/flavours
    contrib/ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

END()
