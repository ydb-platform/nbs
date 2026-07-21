PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_WORKLOAD_PATH="contrib/ydb/tests/stress/testshard_workload/workload_testshard")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/tests/stress/testshard_workload
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/common
)

END()
