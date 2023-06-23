DEPENDS(
    cloud/blockstore/daemon_lightweight
    cloud/blockstore/tests/recipes/local-null
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/local-null/local-null-recipe
)

