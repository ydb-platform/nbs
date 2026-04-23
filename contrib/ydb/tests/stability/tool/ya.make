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
    contrib/ydb/tests/stress/statistics_workload NAME statistics_workload
    contrib/ydb/tools/cfg/bin NAME cfg
    contrib/ydb/tests/tools/nemesis/driver NAME nemesis
    contrib/ydb/apps/ydb NAME ydb_cli
)

RESOURCE(
    ydb_cli ydb_cli
    simple_queue simple_queue
    olap_workload olap_workload
    statistics_workload statistics_workload
    cfg cfg
    nemesis nemesis
    contrib/ydb/tests/stability/resources/tbl_profile.txt tbl_profile.txt
)


PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/wardens
)

END()

