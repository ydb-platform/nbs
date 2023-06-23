DEPENDS(
    cloud/storage/core/tests/recipes/virtiofs-server
    cloud/storage/core/tools/testing/virtiofs_server/bin
)

USE_RECIPE(
    cloud/storage/core/tests/recipes/virtiofs-server/virtiofs-server-recipe
)
