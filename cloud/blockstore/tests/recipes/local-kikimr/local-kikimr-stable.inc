DEPENDS(
    cloud/blockstore/tests/recipes/local-kikimr
    cloud/blockstore/tests/recipes/local-kikimr/stable-package-nbs

    kikimr/public/tools/package/stable
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

USE_RECIPE(
    cloud/blockstore/tests/recipes/local-kikimr/local-kikimr-recipe --kikimr-package-path kikimr/public/tools/package/stable --nbs-package-path cloud/blockstore/tests/recipes/local-kikimr/stable-package-nbs --use-ic-version-check
)

