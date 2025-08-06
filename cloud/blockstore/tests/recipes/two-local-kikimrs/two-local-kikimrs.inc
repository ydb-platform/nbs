DEPENDS(
    cloud/blockstore/apps/server
    cloud/blockstore/tests/recipes/two-local-kikimrs

    contrib/ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/two-local-kikimrs/two-local-kikimrs-recipe --use-log-files
)
