PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
FORK_SUBTESTS()
SPLIT_FACTOR(2)

TEST_SRCS(
    test_vector_index_large_levels_and_clusters.py
    test_vector_index.py
)

PEERDIR(
    contrib/ydb/tests/datashard/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
