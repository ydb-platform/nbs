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
    --service kikimr
    --restart-interval $VHOST_RESTART_INTERVAL
    --restart-flag  $VHOST_RESTART_FLAG
)

IF (NFS_STORAGE_CONFIG_PATCH)
    SET_APPEND(RECIPE_ARGS --storage-config-patch $NFS_STORAGE_CONFIG_PATCH)
ENDIF()

IF (NOT OPENSOURCE OR FORCE_VERBOSE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

IF (USE_VHOST_UNIX_SOCKET)
    SET_APPEND(RECIPE_ARGS --use-unix-socket)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost/vhost-recipe
    ${RECIPE_ARGS}
)
