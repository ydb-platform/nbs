PY3TEST()

    TAG(ya:manual)

    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
    ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")
    ENV(NEMESIS_BINARY="contrib/ydb/tests/stability/nemesis/nemesis")

    PY_SRCS(
        all_workloads.py
    )
    TEST_SRCS (
        test_workload_parallel.py
        test_per_workload.py
    )

    PEERDIR (
        contrib/ydb/tests/library/stability
        contrib/ydb/tests/stress/common
    )

    IF(NOT NOT_INCLUDE_CLI)
        DEPENDS (
            contrib/ydb/apps/ydb
            contrib/ydb/tests/stress/backup
            contrib/ydb/tests/stress/simple_queue
            contrib/ydb/tests/stress/topic
            contrib/ydb/tests/stress/log
            contrib/ydb/tests/stress/mixedpy
            contrib/ydb/tests/stress/kv
            contrib/ydb/tests/stress/oltp_workload
            contrib/ydb/tests/stress/olap_workload
            contrib/ydb/tests/stress/ctas
            contrib/ydb/tests/stress/kafka
            contrib/ydb/tests/stress/node_broker
            contrib/ydb/tests/stress/topic_kafka
            contrib/ydb/tests/stress/transfer
            contrib/ydb/tests/stress/reconfig_state_storage_workload
            contrib/ydb/tests/stress/show_create/view
            contrib/ydb/tests/stress/show_create/table
            contrib/ydb/tests/stress/cdc
            contrib/ydb/tests/stress/statistics_workload
            contrib/ydb/tests/stress/viewer
            contrib/ydb/tests/stress/testshard_workload
            contrib/ydb/tests/stress/streaming
            contrib/ydb/tests/stress/kv_volume
            contrib/ydb/tests/stress/topic_sqs
            contrib/ydb/tests/stability/nemesis
            contrib/ydb/tests/stress/min_max_workload
            contrib/ydb/tests/stress/result_set_format
            contrib/ydb/tests/stress/system_tablet_backup
        )
    ENDIF()

END()
