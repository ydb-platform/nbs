OWNER(g:cloud-nbs)

GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks)

SET(RECIPE_ARGS kikimr)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

SIZE(MEDIUM)

END()
