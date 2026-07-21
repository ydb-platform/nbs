PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_truncate_table_concurrency.py
)

PEERDIR(
    contrib/ydb/tests/datashard/lib
    contrib/ydb/tests/sql/lib
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
