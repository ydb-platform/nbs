DEPENDS(
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
    cloud/filestore/tests/recipes/blockstore-disk-agent
)

SET(BLOCKSTORE_DA_RECIPE_ARGS)

IF (FASTSHARD_DA_AGENT_COUNT)
    SET_APPEND(BLOCKSTORE_DA_RECIPE_ARGS
        --agent-count $FASTSHARD_DA_AGENT_COUNT)
ENDIF()

IF (FASTSHARD_DA_DEVICE_COUNT)
    SET_APPEND(BLOCKSTORE_DA_RECIPE_ARGS
        --device-count $FASTSHARD_DA_DEVICE_COUNT)
ENDIF()

IF (FASTSHARD_DA_DEVICE_SIZE)
    SET_APPEND(BLOCKSTORE_DA_RECIPE_ARGS
        --device-size $FASTSHARD_DA_DEVICE_SIZE)
ENDIF()

# The recipe shares the ydb with service-kikimr.inc, the settings must match
IF (OPENSOURCE)
    SET(YDB_ENABLE_EXTERNAL_DATA_CHANNEL yes)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/ydb.inc)

USE_RECIPE(
    cloud/filestore/tests/recipes/blockstore-disk-agent/blockstore-disk-agent-recipe
    ${BLOCKSTORE_DA_RECIPE_ARGS}
)
# The PY3_PROGRAM built by ya.make.inc is named "blockstore-disk-agent-recipe"
# but lives at .../blockstore-disk-agent/ — DEPENDS points at the source dir,
# USE_RECIPE takes source-dir/binary-name.
