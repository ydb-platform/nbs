PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(19)
SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

TEST_SRCS(
    test_parametrized_queries.py
)

PEERDIR(
    contrib/ydb/tests/sql/lib
    contrib/ydb/tests/datashard/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
