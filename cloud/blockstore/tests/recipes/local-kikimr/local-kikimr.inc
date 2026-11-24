DEPENDS(
    cloud/blockstore/apps/server
    cloud/blockstore/tests/recipes/local-kikimr
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/ydb.inc)

USE_RECIPE(
    cloud/blockstore/tests/recipes/local-kikimr/local-kikimr-recipe
)

