GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks)

SET(RECIPE_ARGS --ydb-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

SIZE(MEDIUM)

END()
