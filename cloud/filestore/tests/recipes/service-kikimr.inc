DEPENDS(
    cloud/filestore/apps/server
    cloud/filestore/tests/recipes/service-kikimr

    cloud/storage/core/tools/testing/ydb/bin
)

IF (NFS_RESTART_INTERVAL)
    DEPENDS(
        cloud/storage/core/tools/testing/unstable-process
    )
ENDIF()

SET(RECIPE_ARGS
    --use-log-files
    --in-memory-pdisks
    --restart-interval $NFS_RESTART_INTERVAL
)

IF (NFS_STORAGE_CONFIG_PATCH)
    SET_APPEND(RECIPE_ARGS --storage-config-patch $NFS_STORAGE_CONFIG_PATCH)
ENDIF()

IF (NOT OPENSOURCE OR NFS_FORCE_VERBOSE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/service-kikimr/service-kikimr-recipe
    ${RECIPE_ARGS}
)
