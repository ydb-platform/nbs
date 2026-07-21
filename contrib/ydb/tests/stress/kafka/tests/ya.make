PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="contrib/ydb/tests/stress/kafka/kafka_streams_test")

TEST_SRCS(
    test_kafka_streams.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/stress/kafka
)

PEERDIR(
    library/python/port_manager
    contrib/ydb/tests/library
    contrib/ydb/tests/library/stress
    contrib/ydb/tests/stress/kafka/workload
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
)

REQUIREMENTS(network:full)

END()
