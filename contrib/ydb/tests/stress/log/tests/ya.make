PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="contrib/ydb/tests/stress/log/workload_log")

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

REQUIREMENTS(ram:32 cpu:4)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/log
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
)

END()
