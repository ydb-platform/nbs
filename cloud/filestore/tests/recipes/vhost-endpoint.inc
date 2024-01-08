DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tests/recipes/vhost-endpoint
)

SET(RECIPE_ARGS --filesystem nfs_test)

IF (NOT OPENSOURCE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost-endpoint/vhost-endpoint-recipe
    ${RECIPE_ARGS}
)
