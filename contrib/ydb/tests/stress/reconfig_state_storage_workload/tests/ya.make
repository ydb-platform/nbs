PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(STRESS_TEST_UTILITY="contrib/ydb/tests/stress/reconfig_state_storage_workload/reconfig_state_storage_workload")

TEST_SRCS(
    reconfig_state_storage_workload_test.py
    test_board_workload.py
    test_scheme_board_workload.py
    test_state_storage_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(IAM_TOKEN="")

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/reconfig_state_storage_workload
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/clients
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/common
    contrib/ydb/tests/stress/reconfig_state_storage_workload/workload
)

END()
