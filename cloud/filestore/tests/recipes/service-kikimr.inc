DEPENDS(
    cloud/filestore/apps/server
    cloud/filestore/tests/recipes/service-kikimr
)

IF (NFS_RESTART_INTERVAL)
    DEPENDS(
        cloud/storage/core/tools/testing/unstable-process
    )
ENDIF()

SET(RECIPE_ARGS
    --restart-interval $NFS_RESTART_INTERVAL
)

# Use XDC only in open-source builds as YDB Storage doesn't enable it by default
IF (OPENSOURCE)
    SET_APPEND(RECIPE_ARGS --enable-external-data-channel)
    SET(YDB_ENABLE_EXTERNAL_DATA_CHANNEL yes)
ENDIF()

IF (NFS_STORAGE_CONFIG_PATCH)
    SET_APPEND(RECIPE_ARGS --storage-config-patch $NFS_STORAGE_CONFIG_PATCH)
ENDIF()

IF (NFS_DIAG_CONFIG_PATCH)
    SET_APPEND(RECIPE_ARGS --diag-config-patch $NFS_DIAG_CONFIG_PATCH)
ENDIF()

IF (NFS_SERVER_CONFIG_PATCH)
    SET_APPEND(RECIPE_ARGS --server-config-patch $NFS_SERVER_CONFIG_PATCH)
ENDIF()

IF (NOT OPENSOURCE OR NFS_FORCE_VERBOSE)
    SET_APPEND(RECIPE_ARGS --verbose)
ENDIF()

IF (USE_UNIX_SOCKET)
    SET_APPEND(RECIPE_ARGS --use-unix-socket)
ENDIF()

IF (NFS_TRACE_SAMPLING_RATE)
    SET_APPEND(RECIPE_ARGS --trace-sampling-rate $NFS_TRACE_SAMPLING_RATE)
ENDIF()

IF (USE_FAST_SHARD_PORT)
    SET_APPEND(RECIPE_ARGS --use-fast-shard-port)
ENDIF()

IF (NFS_BS_FAILURE_PROBABILITY)
    SET_APPEND(RECIPE_ARGS --bs-failure-probability $NFS_BS_FAILURE_PROBABILITY)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/ydb.inc)

USE_RECIPE(
    cloud/filestore/tests/recipes/service-kikimr/service-kikimr-recipe
    ${RECIPE_ARGS}
)
