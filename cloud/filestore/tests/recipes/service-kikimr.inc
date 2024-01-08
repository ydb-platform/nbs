DEPENDS(
    cloud/filestore/apps/server
    cloud/filestore/tests/recipes/service-kikimr

    contrib/ydb/apps/ydbd
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

IF (NOT OPENSOURCE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/service-kikimr/service-kikimr-recipe
    ${RECIPE_ARGS}
)
