PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(45)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
TEST_SRCS(
    conftest.py
    test_split_merge_by_load.py
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
