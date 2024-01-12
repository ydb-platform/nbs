DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/tests/recipes/endpoint
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/endpoint/endpoint-recipe --ipc-type=nbd
)
