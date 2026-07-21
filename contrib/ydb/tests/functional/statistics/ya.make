PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

PY_SRCS (
    conftest.py
)

TEST_SRCS(
    test_restarts.py
    test_analyze.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
