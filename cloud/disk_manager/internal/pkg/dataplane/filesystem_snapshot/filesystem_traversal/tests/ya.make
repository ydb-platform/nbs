GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/filesystem_traversal)

SET_APPEND(RECIPE_ARGS --nfs-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)
REQUIREMENTS(
    ram:16
)

FORK_SUBTESTS()
SPLIT_FACTOR(16)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
