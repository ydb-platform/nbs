DEPENDS(
    cloud/blockstore/tests/recipes/fake-root-kms
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/fake-root-kms/dependencies.inc)

USE_RECIPE(cloud/blockstore/tests/recipes/fake-root-kms/fake-root-kms ${RECIPE_ARGS})
