DEPENDS(
    cloud/storage/core/tests/recipes/ydb
    cloud/storage/core/tools/testing/ydb/bin
)

DEFAULT(YDB_BINARY_PATH cloud/storage/core/tools/testing/ydb/bin/ydbd)
DEFAULT(YDB_DYNAMIC_STORAGE_POOL_KINDS rot ssd)
DEFAULT(YDB_STATIC_PDISK_SIZE 68719476736)

SET(YDB_RECIPE_ARGS
    --binary-path $YDB_BINARY_PATH
    --dynamic-storage-pool-kinds $YDB_DYNAMIC_STORAGE_POOL_KINDS
    --static-pdisk-size $YDB_STATIC_PDISK_SIZE
)

IF (YDB_ENABLE_EXTERNAL_DATA_CHANNEL)
    SET_APPEND(YDB_RECIPE_ARGS --enable-external-data-channel)
ENDIF()

IF (YDB_BS_CACHE_FILE_PATH)
    SET_APPEND(YDB_RECIPE_ARGS --bs-cache-file-path $YDB_BS_CACHE_FILE_PATH)
ENDIF()

USE_RECIPE(
    cloud/storage/core/tests/recipes/ydb/ydb-recipe
    ${YDB_RECIPE_ARGS}
)
