DEPENDS(
    cloud/blockstore/apps/server_lightweight
    cloud/blockstore/tests/recipes/service-local
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/service-local/service-local-recipe
)

