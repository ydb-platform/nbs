DEPENDS(
    contrib/ydb/tests/fq/streaming_common/iam_grpc_emulator/bin
    contrib/ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe
)

USE_RECIPE(
    contrib/ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe/iam_grpc_recipe
)
