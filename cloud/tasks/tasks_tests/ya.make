GO_TEST()

ENV(YDB_ALLOCATE_PGWIRE_PORT=true)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

GO_XTEST_SRCS(
    tasks_test.go
)

SIZE(MEDIUM)

END()
