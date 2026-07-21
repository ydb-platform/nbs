
PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(STRESS_TEST_UTILITY="contrib/ydb/tests/stress/show_create/table/show_create_table")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/show_create/table
)

PEERDIR(
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/show_create/table/workload
)

END()
