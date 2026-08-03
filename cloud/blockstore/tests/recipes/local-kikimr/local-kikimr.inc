DEPENDS(
    cloud/blockstore/apps/server
    cloud/blockstore/tests/recipes/local-kikimr

    contrib/ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

IF (NOT LOCAL_KIKIMR_SERVER_COUNT)
    SET(LOCAL_KIKIMR_SERVER_COUNT 1)
ENDIF()

USE_RECIPE(
    cloud/blockstore/tests/recipes/local-kikimr/local-kikimr-recipe
    --use-log-files
    --server-count $LOCAL_KIKIMR_SERVER_COUNT
)
