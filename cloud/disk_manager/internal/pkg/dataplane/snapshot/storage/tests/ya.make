GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/snapshot/storage)

SET_APPEND(RECIPE_ARGS --ydb-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(16)

SIZE(LARGE)

END()
