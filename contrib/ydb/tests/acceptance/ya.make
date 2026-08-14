# Various tests which we can't run in every pull request (because of instability/specific environment/execution time/etc)

PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLUSTER_YAML="contrib/ydb/tests/acceptance/cluster.yaml")

TEST_SRCS(
    test_slice.py
)

TAG(ya:fat)
SIZE(LARGE)

DEPENDS(
    contrib/ydb/tests/tools/ydb_serializable
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tools/ydbd_slice
)

DATA(
    arcadia/contrib/ydb/tests/acceptance/cluster.yaml
)

END()
