DEPENDS(
    cloud/blockstore/tests/recipes/nvme-loop
)

PEERDIR(
)

USE_RECIPE(cloud/blockstore/tests/recipes/nvme-loop/nvme-loop ${RECIPE_ARGS})
