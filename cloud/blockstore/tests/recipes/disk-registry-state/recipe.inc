SET(RECIPE_DEPS_LIST
    cloud/blockstore/tests/recipes/disk-registry-state
    cloud/blockstore/tools/testing/disk-registry-state-generator
)

DEPENDS(${RECIPE_DEPS_LIST})

DATA(
    arcadia/cloud/blockstore/tests/recipes/disk-registry-state/data
)

USE_RECIPE(cloud/blockstore/tests/recipes/disk-registry-state/recipe)
