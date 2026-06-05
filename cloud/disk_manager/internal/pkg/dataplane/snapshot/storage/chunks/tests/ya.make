GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks)

SET_APPEND(RECIPE_ARGS --ydb-only)
SET_APPEND(RECIPE_ARGS --s3-quota STANDARD_IA:8)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

SIZE(MEDIUM)

END()
