DEPENDS(
    contrib/python/moto/bin
    contrib/ydb/tests/tools/s3_recipe
)

USE_RECIPE(
    contrib/ydb/tests/tools/s3_recipe/s3_recipe
)
