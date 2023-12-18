DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tests/recipes/vhost-endpoint
)

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost-endpoint/vhost-endpoint-recipe
        --filesystem nfs_test
        --verbose
)
