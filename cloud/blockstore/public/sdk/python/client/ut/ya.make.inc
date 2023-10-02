SET(source_dir cloud/blockstore/public/sdk/python/client/ut)

TEST_SRCS(
    ${source_dir}/client_ut.py
    ${source_dir}/durable_ut.py
    ${source_dir}/discovery_ut.py
    ${source_dir}/future_ut.py
    ${source_dir}/grpc_client_ut.py
    ${source_dir}/http_client_ut.py
    ${source_dir}/session_ut.py
)

USE_RECIPE(cloud/blockstore/tests/recipes/local-null/local-null-recipe with-http-proxy)

PEERDIR(
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/blockstore/public/sdk/python/client
    cloud/blockstore/public/sdk/python/client/ut

    library/python/testing/yatest_common
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

DEPENDS(
    cloud/blockstore/daemon_lightweight
    cloud/blockstore/tests/recipes/local-null
    cloud/blockstore/tools/http_proxy
)
