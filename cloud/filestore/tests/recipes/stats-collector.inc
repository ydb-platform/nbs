DEPENDS(
    cloud/filestore/tests/recipes/stats-collector
)

IF (FILESTORE_STATS_COLLECTOR_PERIOD)
    SET(RECIPE_ARGS --period $FILESTORE_STATS_COLLECTOR_PERIOD)
ELSE()
    MESSAGE(FATAL_ERROR FILESTORE_STATS_COLLECTOR_PERIOD should be set for the stats-collector recipe to work)
ENDIF()

IF (FILESTORE_STATS_COLLECTOR_ENDPOINT)
    SET_APPEND(RECIPE_ARGS --endpoint $FILESTORE_STATS_COLLECTOR_ENDPOINT)
ELSE()
    MESSAGE(FATAL_ERROR FILESTORE_STATS_COLLECTOR_ENDPOINT should be set for the stats-collector recipe to work)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/stats-collector/filestore-stats-collector
    ${RECIPE_ARGS}
)
