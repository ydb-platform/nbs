GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing)

SET_APPEND(RECIPE_ARGS --nfs-only)
SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --allow-filestore-force-destroy)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

SIZE(LARGE)
TAG(ya:fat)

IF (RACE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ENDIF()

END()
