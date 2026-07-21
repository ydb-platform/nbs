PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_add_column.py
)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/tests/datashard/lib
    contrib/ydb/tests/sql/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

END()
