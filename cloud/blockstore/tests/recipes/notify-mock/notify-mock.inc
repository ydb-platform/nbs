DEPENDS(
    cloud/blockstore/tests/recipes/notify-mock
    cloud/blockstore/tools/testing/notify-mock
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/notify-mock/notify-mock-recipe secure
)
