GO_TEST_FOR(cloud/disk_manager/internal/pkg/services/shards/storage)

SET_APPEND(RECIPE_ARGS --ydb-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
