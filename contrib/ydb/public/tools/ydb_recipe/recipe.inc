ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")

DEPENDS(
    contrib/ydb/apps/ydbd
    contrib/ydb/public/tools/ydb_recipe
)

USE_RECIPE(
    contrib/ydb/public/tools/ydb_recipe/ydb_recipe --suppress-version-check
)
