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
    --use-cells
    --nbs-index 0
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/local-kikimr/local-kikimr-recipe
    --use-log-files
    --use-cells
    --nbs-index 1
)
