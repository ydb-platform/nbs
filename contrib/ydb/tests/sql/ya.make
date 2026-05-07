PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_ENABLE_COLUMN_TABLES="true")

TEST_SRCS(
    test_kv.py
    test_crud.py
    test_inserts.py
)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/sql/lib
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/sql/lib
)

END()

RECURSE(
    lib
    large
)
