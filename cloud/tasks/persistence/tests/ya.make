GO_TEST_FOR(cloud/tasks/persistence)

ENV(YDB_ALLOCATE_PGWIRE_PORT=true)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

END()
