DEPENDS(
    cloud/filestore/apps/vhost
    cloud/filestore/tests/recipes/vhost
)

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost/vhost-recipe --service=null
)
