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

USE_RECIPE(
    cloud/filestore/tests/recipes/service-kikimr/service-kikimr-recipe
        --use-log-files
        --in-memory-pdisks
        --restart-interval $NFS_RESTART_INTERVAL
        --verbose
)
