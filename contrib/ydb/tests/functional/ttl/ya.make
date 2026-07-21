PY3TEST()

TEST_SRCS(
    test_ttl.py
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

DEPENDS(
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
