DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/apps/vhost
    cloud/storage/core/tests/recipes/virtiofs-server
    cloud/storage/core/tools/testing/virtiofs_server/bin
)

USE_RECIPE(
    cloud/storage/core/tests/recipes/virtiofs-server/virtiofs-server-recipe
    --server-count $VIRTIOFS_SERVER_COUNT
)
