DEPENDS(
    cloud/filestore/tests/recipes/tablets-restarter
)

IF (FILESTORE_TABLETS_RESTART_INTERVAL)
    SET(RECIPE_ARGS --restart-interval $FILESTORE_TABLETS_RESTART_INTERVAL)
ELSE()
    MESSAGE(FATAL_ERROR FILESTORE_TABLETS_RESTART_INTERVAL should be set for tablets-restarter recipe to work)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/tablets-restarter/filestore-tablets-restarter
    ${RECIPE_ARGS}
)
