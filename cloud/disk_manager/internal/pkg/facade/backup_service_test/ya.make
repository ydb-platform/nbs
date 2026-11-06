GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --creation-and-deletion-allowed-only-for-disks-with-id-prefix "Test")
SET_APPEND(RECIPE_ARGS --disk-agent-count 5)
SET_APPEND(RECIPE_ARGS --backup-test-stand)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

DEPENDS(
    cloud/disk_manager/test/mocks/backup-fault-proxy
)

GO_XTEST_SRCS(
    deletion_test.go
    images_test.go
    fixture_test.go
    backup_test.go
    lifecycle_test.go
    slow_test.go
)

FORK_SUBTESTS()
SPLIT_FACTOR(4)
SIZE(LARGE)
TIMEOUT(3600)
TAG(ya:fat ya:force_sandbox ya:sandbox_coverage sb:ssd)

REQUIREMENTS(
    cpu:8
    ram:24
    disk_usage:200
)

END()
