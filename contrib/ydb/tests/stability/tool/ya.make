PY3_PROGRAM()

PY_SRCS(
    __main__.py
)

DATA(
    arcadia/contrib/ydb/tests/stability/resources
)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tools/cfg/bin
    contrib/ydb/tests/tools/nemesis/driver
)

BUNDLE(
    contrib/ydb/tests/stress/simple_queue NAME simple_queue
    contrib/ydb/tests/stress/olap_workload NAME olap_workload
    contrib/ydb/tests/stress/oltp_workload NAME oltp_workload
    contrib/ydb/tests/stress/statistics_workload NAME statistics_workload
    contrib/ydb/tests/stress/node_broker NAME node_broker_workload
    contrib/ydb/tests/stress/transfer NAME transfer_workload
    contrib/ydb/tests/stress/s3_backups NAME s3_backups_workload
    contrib/ydb/tests/stress/ctas NAME ctas_workload
    contrib/ydb/tests/stress/topic_kafka NAME topic_kafka_workload
    contrib/ydb/tests/stress/kafka NAME kafka_workload
    contrib/ydb/tests/stress/topic NAME topic_workload
    contrib/ydb/tools/cfg/bin NAME cfg
    contrib/ydb/tests/tools/nemesis/driver NAME nemesis
    contrib/ydb/apps/ydb NAME ydb_cli
)

RESOURCE(
    ydb_cli ydb_cli
    simple_queue simple_queue
    olap_workload olap_workload
    oltp_workload oltp_workload
    statistics_workload statistics_workload
    node_broker_workload node_broker_workload
    transfer_workload transfer_workload
    s3_backups_workload s3_backups_workload
    ctas_workload ctas_workload
    topic_kafka_workload topic_kafka_workload
    kafka_workload kafka_workload
    topic_workload topic_workload
    cfg cfg
    nemesis nemesis
    contrib/ydb/tests/stability/resources/tbl_profile.txt tbl_profile.txt
)


PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/wardens
)

END()

