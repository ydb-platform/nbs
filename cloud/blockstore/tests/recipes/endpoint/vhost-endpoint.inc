DEPENDS(
    cloud/blockstore/client
    cloud/blockstore/tests/recipes/endpoint
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/endpoint/endpoint-recipe --ipc-type=vhost
)
