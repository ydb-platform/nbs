PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/large.inc)

TEST_SRCS(
    keepalive.py
    test_with_client.py
    test_with_multiple_agents.py
)

DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server

    ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/client
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
    cloud/storage/core/protos
    ydb/core/protos
    ydb/tests/library
)

END()
