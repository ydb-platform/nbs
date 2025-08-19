DEPENDS(
    cloud/blockstore/apps/server
    cloud/blockstore/tests/recipes/local-kikimr

    contrib/ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/local-kikimr/local-kikimr-recipe
    --use-log-files
    --nbs-instances-count $NBS_INSTANCES_COUNT
)
