PY3TEST()

SIZE(LARGE)
TIMEOUT(3600)
TAG(ya:fat)

REQUIREMENTS(
    cpu:8
    ram:16
)

DEPENDS(
    cloud/blockstore/apps/server
    contrib/ydb/apps/ydb
    contrib/ydb/apps/ydbd
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/public/api/grpc
    cloud/blockstore/tests/python/lib
    cloud/storage/core/protos
    contrib/proto/opentelemetry
    contrib/ydb/core/protos
    contrib/ydb/tests/library
)

PY_SRCS(
    tracing_harness.py
    tracing_perf.py
)

TEST_SRCS(
    test.py
)

END()
