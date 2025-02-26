PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

PY_SRCS(
    client_methods.py
)

TEST_SRCS(
    client_ut.py
    durable_ut.py
    discovery_ut.py
    future_ut.py
    grpc_client_ut.py
    session_ut.py
)

USE_RECIPE(cloud/blockstore/tests/recipes/local-null/local-null-recipe)

PEERDIR(
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/blockstore/public/sdk/python/client

    library/python/testing/yatest_common
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

DEPENDS(
    cloud/blockstore/apps/server_lightweight
    cloud/blockstore/tests/recipes/local-null
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/opensource.inc)

END()
