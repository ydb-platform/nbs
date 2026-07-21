PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(23)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

TEST_SRCS(
    test_partitioning.py

)

PEERDIR(
    contrib/ydb/tests/datashard/lib
    contrib/ydb/tests/sql/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
