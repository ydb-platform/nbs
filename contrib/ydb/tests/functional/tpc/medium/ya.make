PY3TEST()
ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")

TEST_SRCS(
    test_clickbench.py
    test_tpch.py
)

SIZE(MEDIUM)

REQUIREMENTS(ram:16)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(NO_KUBER_LOGS="yes")

PEERDIR(
    contrib/ydb/tests/functional/tpc/lib
)

DEPENDS(
    contrib/ydb/apps/ydb
)

DATA(
    arcadia/contrib/ydb/tests/functional/clickbench/data/hits.csv
)

FORK_TEST_FILES()

END()
