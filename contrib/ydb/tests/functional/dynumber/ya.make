PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)
TIMEOUT(600)
SIZE(MEDIUM)

TEST_SRCS(
    test_dynumber.py
)

PEERDIR(
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/ydb/public/sdk/python
)

END()
