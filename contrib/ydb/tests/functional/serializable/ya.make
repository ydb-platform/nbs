PY3TEST()

ENV(YDB_CHANNEL_BUFFER_SIZE="8388608")

PEERDIR(
    contrib/ydb/tests/tools/ydb_serializable/lib
    contrib/ydb/public/sdk/python
)

TEST_SRCS(test.py)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)
END()
