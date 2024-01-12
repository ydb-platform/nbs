GO_TEST_FOR(cloud/disk_manager/internal/pkg/tasks/persistence)

SET(RECIPE_ARGS --kikimr-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

SIZE(MEDIUM)

END()
