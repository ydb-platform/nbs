DEPENDS(
    cloud/blockstore/apps/client
    cloud/nbs_internal/blockstore/daemon

    cloud/nbs_internal/filestore/server

    cloud/disk_manager/cmd/disk-manager
    cloud/disk_manager/cmd/disk-manager-init-db
    cloud/disk_manager/test/images/recipe
    cloud/disk_manager/test/mocks/accessservice
    cloud/disk_manager/test/mocks/compute
    cloud/disk_manager/test/mocks/kms
    cloud/disk_manager/test/mocks/metadata
    cloud/disk_manager/test/nemesis
    cloud/disk_manager/test/recipe

    contrib/python/moto/bin
    library/recipes/s3mds

    contrib/ydb/apps/ydbd
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

USE_RECIPE(library/recipes/s3mds/s3mds-recipe)

USE_RECIPE(cloud/disk_manager/test/recipe/recipe ${RECIPE_ARGS})
