# Various tests which we can't run in every pull request (because of instability/specific environment/execution time/etc)

PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENV(YDB_CLUSTER_YAML="contrib/ydb/tests/acceptance/cluster.yaml")

TEST_SRCS(
    test_slice.py
)

TAG(ya:fat)
SIZE(LARGE)

DEPENDS(
    contrib/ydb/tests/tools/ydb_serializable
    contrib/ydb/apps/ydbd
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tools/ydbd_slice
)

DATA(
    arcadia/contrib/ydb/tests/acceptance/cluster.yaml
)

END()
