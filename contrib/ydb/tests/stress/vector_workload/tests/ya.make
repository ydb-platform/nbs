PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_WORKLOAD_PATH="contrib/ydb/tests/stress/vector_workload/workload_vector")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)
TAG(ya:external)

SIZE(LARGE)
TAG(ya:fat)
TIMEOUT(1200)

DEPENDS(
    contrib/ydb/tests/stress/vector_workload
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
)

END()
