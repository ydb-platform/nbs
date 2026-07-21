PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
SIZE(MEDIUM)

REQUIREMENTS(cpu:4)

TEST_SRCS(
    conftest.py
    test_canonical_requests.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/apps/dstool
    contrib/ydb/apps/dstool/lib
)

END()
