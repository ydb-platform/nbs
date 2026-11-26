DEPENDS(cloud/storage/core/tests/recipes/virtiofs-server)

IF (VIRTIOFS_SERVER_FROM_SOURCE)
    DEPENDS(
        cloud/filestore/apps/client
        cloud/filestore/apps/vhost
    )
    ENV(VIRTIOFS_SERVER_BINARY=cloud/filestore/apps/vhost/filestore-vhost)
    ENV(VIRTIOFS_CLIENT_BINARY=cloud/filestore/apps/client/filestore-client)
ELSE()
    DEPENDS(cloud/storage/core/tools/testing/virtiofs_server/filestore-bin)
    ENV(VIRTIOFS_SERVER_BINARY=cloud/storage/core/tools/testing/virtiofs_server/filestore-bin/filestore-vhost)
    ENV(VIRTIOFS_CLIENT_BINARY=cloud/storage/core/tools/testing/virtiofs_server/filestore-bin/filestore-client)
ENDIF()

USE_RECIPE(
    cloud/storage/core/tests/recipes/virtiofs-server/virtiofs-server-recipe
    --server-count $VIRTIOFS_SERVER_COUNT
)
