DEPENDS(
    cloud/blockstore/tests/recipes/notify-mock
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/blockstore/tools/testing/notify-mock/__main__.py
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/notify-mock/notify-mock-recipe --secure
)
