PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="contrib/ydb/tests/stress/backup/backup_stress")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)
SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/tests/stress/backup
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/backup/workload
)

END()