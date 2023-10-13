SET(RECIPE_DEPS_LIST
    cloud/blockstore/apps/client

    cloud/filestore/server

    cloud/disk_manager/cmd/yc-disk-manager
    cloud/disk_manager/cmd/yc-disk-manager-init-db
    cloud/disk_manager/test/images/recipe
    cloud/disk_manager/test/mocks/accessservice
    cloud/disk_manager/test/mocks/compute
    cloud/disk_manager/test/mocks/kms
    cloud/disk_manager/test/mocks/metadata
    cloud/disk_manager/test/nemesis
    cloud/disk_manager/test/recipe

    contrib/python/moto/bin
    library/recipes/s3mds
)

IF (STABLE_DEPS_RECIPE)
    SET_APPEND(RECIPE_ARGS stable)
    SET_APPEND(RECIPE_DEPS_LIST
        kikimr/public/tools/package/stable
        cloud/blockstore/tests/recipes/local-kikimr/stable-package-nbs
    )
ELSE()
    SET_APPEND(RECIPE_DEPS_LIST
        kikimr/driver
        cloud/nbs_internal/blockstore/daemon
    )
ENDIF()

DEPENDS(${RECIPE_DEPS_LIST})

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

USE_RECIPE(library/recipes/s3mds/s3mds-recipe)

USE_RECIPE(cloud/disk_manager/test/recipe/recipe ${RECIPE_ARGS})
