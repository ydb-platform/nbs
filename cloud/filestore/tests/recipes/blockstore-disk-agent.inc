DEPENDS(
    cloud/filestore/tests/recipes/blockstore-disk-agent
)

SET(RECIPE_ARGS
    --device-count $FASTSHARD_DA_DEVICE_COUNT
    --device-size $FASTSHARD_DA_DEVICE_SIZE
)

USE_RECIPE(
    cloud/filestore/tests/recipes/blockstore-disk-agent/blockstore-disk-agent-recipe
    ${RECIPE_ARGS}
)
