DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tests/recipes/vhost-endpoint
)

SET(RECIPE_ARGS --filesystem nfs_test)

IF (NOT OPENSOURCE OR NFS_FORCE_VERBOSE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

IF (FILESTORE_SHARD_COUNT)
    SET_APPEND(RECIPE_ARGS --shard-count $FILESTORE_SHARD_COUNT)
ENDIF()

IF (FILESTORE_VHOST_ENDPOINT_COUNT)
    SET_APPEND(RECIPE_ARGS --endpoint-count $FILESTORE_VHOST_ENDPOINT_COUNT)
ENDIF()

IF (FILESTORE_VHOST_QUEUE_COUNT)
    SET_APPEND(RECIPE_ARGS --vhost-queue-count $FILESTORE_VHOST_QUEUE_COUNT)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/vhost-endpoint/vhost-endpoint-recipe
    ${RECIPE_ARGS}
)
