DEPENDS(
    cloud/storage/core/tests/recipes/virtiofs-server
    cloud/filestore/apps/client
    cloud/filestore/apps/vhost
)

USE_RECIPE(
    cloud/storage/core/tests/recipes/virtiofs-server/virtiofs-server-recipe
    --server-count $VIRTIOFS_SERVER_COUNT
)
