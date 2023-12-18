DEPENDS(
    cloud/filestore/apps/vhost
    cloud/filestore/tests/recipes/vhost
)

IF (VHOST_RESTART_INTERVAL)
    DEPENDS(
        cloud/storage/core/tools/testing/unstable-process
    )
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost/vhost-recipe
        --restart-interval $VHOST_RESTART_INTERVAL
        --restart-flag  $VHOST_RESTART_FLAG
        --verbose
)
