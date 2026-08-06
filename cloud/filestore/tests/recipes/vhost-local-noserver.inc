DEPENDS(
    cloud/filestore/apps/vhost
    cloud/filestore/tests/recipes/vhost
)

IF (VHOST_RESTART_INTERVAL)
    DEPENDS(
        cloud/storage/core/tools/testing/unstable-process
    )
ENDIF()

SET(RECIPE_ARGS
    --service local-noserver
    --restart-interval $VHOST_RESTART_INTERVAL
    --restart-flag $VHOST_RESTART_FLAG
    --local-service-config-patch cloud/filestore/tests/common_configs/nfs-vhost-local-common-patch.txt
)

IF (NFS_LOCAL_SERVICE_CONFIG_PATCH)
    SET_APPEND(RECIPE_ARGS --local-service-config-patch $NFS_LOCAL_SERVICE_CONFIG_PATCH)
ENDIF()

IF (NOT OPENSOURCE OR NFS_FORCE_VERBOSE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost/vhost-recipe
    ${RECIPE_ARGS}
)
