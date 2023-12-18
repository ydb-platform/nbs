DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tests/recipes/mount
)

USE_RECIPE(
    cloud/filestore/tests/recipes/mount/mount-recipe
        --filesystem nfs_share
        --verbose
)
