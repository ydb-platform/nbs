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
)

IF (VHOST_DIRECT_IO)
    SET_APPEND(RECIPE_ARGS --direct-io)
ENDIF()

IF (NOT OPENSOURCE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost/vhost-recipe
    ${RECIPE_ARGS}
)
