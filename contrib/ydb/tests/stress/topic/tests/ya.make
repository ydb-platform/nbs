PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="contrib/ydb/tests/stress/topic/workload_topic")

TEST_SRCS(
    test_workload_topic.py
)

SIZE(MEDIUM)
REQUIREMENTS(ram:32 cpu:4)

IF (SANITIZER_TYPE)
    ENV(YDB_STRESS_TEST_LIMIT_MEMORY=1)
ENDIF()

FORK_SUBTESTS()

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/topic
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/topic/workload
)

END()
