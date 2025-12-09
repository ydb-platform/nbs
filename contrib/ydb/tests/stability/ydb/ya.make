PY3TEST()

TEST_SRCS(
    test_stability.py
)

SIZE(LARGE)
TAG(ya:manual)

DATA(
    arcadia/contrib/ydb/tests/stability/resources
)

DEPENDS(
    contrib/ydb/tests/stress/simple_queue
    contrib/ydb/tests/stress/olap_workload
    contrib/ydb/tests/stress/statistics_workload
    contrib/ydb/tools/cfg/bin
    contrib/ydb/tests/tools/nemesis/driver
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/wardens
)

END()

