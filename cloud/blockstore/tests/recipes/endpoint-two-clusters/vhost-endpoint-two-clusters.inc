DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/tests/recipes/endpoint-two-clusters
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/endpoint-two-clusters/endpoint-two-clusters-recipe --ipc-type=vhost
)
