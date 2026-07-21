DEPENDS(
    contrib/ydb/tests/fq/streaming_common/vm_metadata_emulator/bin
    contrib/ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe
)

USE_RECIPE(
    contrib/ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe/iam_recipe
)
