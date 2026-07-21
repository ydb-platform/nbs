PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="contrib/ydb/tests/stress/topic_kafka/workload_topic_kafka")

TEST_SRCS(
    test_workload_topic.py
)

SIZE(MEDIUM)
REQUIREMENTS(ram:32 cpu:4)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/topic_kafka
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
)


END()
