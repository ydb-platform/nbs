DEPENDS(
    cloud/filestore/apps/server
    cloud/filestore/tests/recipes/service-local
)

SET_APPEND(RECIPE_ARGS --service local)

IF (NFS_LOCAL_SERVICE_CONFIG_PATCH)
    SET_APPEND(
        RECIPE_ARGS --local-service-config-patch
        $NFS_LOCAL_SERVICE_CONFIG_PATCH
    )
ENDIF()

IF (SERVICE_LOCAL_FILE_IO)
    SET_APPEND(RECIPE_ARGS --file-io $SERVICE_LOCAL_FILE_IO)
ENDIF()


USE_RECIPE(
    cloud/filestore/tests/recipes/service-local/service-local-recipe
    ${RECIPE_ARGS}
)
