DEPENDS(
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    cloud/filestore/tests/recipes/blockstore-disk-agent

    contrib/ydb/apps/ydbd
)

SET(BLOCKSTORE_DA_RECIPE_ARGS)

IF (FASTSHARD_DA_DEVICE_COUNT)
    SET_APPEND(BLOCKSTORE_DA_RECIPE_ARGS
        --device-count $FASTSHARD_DA_DEVICE_COUNT)
ENDIF()

IF (FASTSHARD_DA_DEVICE_SIZE)
    SET_APPEND(BLOCKSTORE_DA_RECIPE_ARGS
        --device-size $FASTSHARD_DA_DEVICE_SIZE)
ENDIF()

USE_RECIPE(
    cloud/filestore/tests/recipes/blockstore-disk-agent/blockstore-disk-agent-recipe
    ${BLOCKSTORE_DA_RECIPE_ARGS}
)
# The PY3_PROGRAM built by ya.make.inc is named "blockstore-disk-agent-recipe"
# but lives at .../blockstore-disk-agent/ — DEPENDS points at the source dir,
# USE_RECIPE takes source-dir/binary-name.
