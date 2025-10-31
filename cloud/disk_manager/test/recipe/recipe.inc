DEPENDS(
    cloud/blockstore/apps/client
    cloud/blockstore/apps/server
    cloud/blockstore/apps/disk_agent

    cloud/filestore/apps/server

    cloud/disk_manager/cmd/disk-manager
    cloud/disk_manager/cmd/disk-manager-init-db
    cloud/disk_manager/test/mocks/compute
    cloud/disk_manager/test/mocks/kms
    cloud/disk_manager/test/mocks/metadata
    cloud/disk_manager/test/recipe
    cloud/tasks/test/nemesis
    contrib/python/moto/bin
)

IF (OPENSOURCE)
    DEPENDS(
        cloud/storage/core/tools/testing/ydb/bin
    )
ELSE()
    DEPENDS(
        ydb/apps/ydbd
    )
ENDIF()

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/filestore/tests/certs/server.crt
    arcadia/cloud/filestore/tests/certs/server.key
)

USE_RECIPE(cloud/disk_manager/test/recipe/recipe ${RECIPE_ARGS})
