DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/tests/recipes/resize-disk
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/resize-disk/resize-disk-recipe
)
