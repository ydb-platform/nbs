PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_min_max_index.py
)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/tests/sql/lib
    contrib/ydb/tests/olap/scenario/helpers
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
