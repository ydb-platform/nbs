PY3TEST()

DATA(
    arcadia/contrib/ydb/tests/tools/kqprun/tests/cfg
)

TEST_SRCS(
    test_kqprun_recipe.py
)

PEERDIR(
    contrib/ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
    contrib/ydb/tests/tools/kqprun
    contrib/ydb/tests/tools/kqprun/recipe
)

USE_RECIPE(
    contrib/ydb/tests/tools/kqprun/recipe/kqprun_recipe 
        --config contrib/ydb/tests/tools/kqprun/tests/cfg/config.conf
        --query contrib/ydb/tests/tools/kqprun/tests/cfg/create_tables.sql
        --query contrib/ydb/tests/tools/kqprun/tests/cfg/fill_tables.sql
)

SIZE(MEDIUM)

END()
