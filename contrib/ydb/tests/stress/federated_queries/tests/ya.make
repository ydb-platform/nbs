PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="contrib/ydb/tests/stress/federated_queries/federated_queries")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/federated_queries
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/federated_queries/workload
)

END()
