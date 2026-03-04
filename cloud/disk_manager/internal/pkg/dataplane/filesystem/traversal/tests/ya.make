GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal)

SET_APPEND(RECIPE_ARGS --nfs-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)
REQUIREMENTS(
    ram:16
)

PEERDIR(
    cloud/disk_manager/internal/pkg/clients/nfs/mocks
    cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage/mocks
    cloud/filestore/public/sdk/go/client
)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
