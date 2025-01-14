DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/tests/recipes/mount
)

SET(RECIPE_ARGS --filesystem nfs_share)

IF (NOT OPENSOURCE OR NFS_FORCE_VERBOSE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/mount/mount-recipe
    ${RECIPE_ARGS}
)
