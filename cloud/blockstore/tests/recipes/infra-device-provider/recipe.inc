DEPENDS(
    cloud/blockstore/tests/recipes/infra-device-provider
    cloud/blockstore/tools/testing/infra-device-provider/server
)

PEERDIR(
)

USE_RECIPE(cloud/blockstore/tests/recipes/infra-device-provider/infra-device-provider ${RECIPE_ARGS})
