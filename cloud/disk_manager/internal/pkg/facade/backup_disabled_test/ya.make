GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --creation-and-deletion-allowed-only-for-disks-with-id-prefix "Test")
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_XTEST_SRCS(
    ../backup_service_test/fixture_test.go
    disabled_test.go
)

SIZE(LARGE)
TAG(ya:fat ya:force_sandbox ya:sandbox_coverage sb:ssd)
REQUIREMENTS(cpu:8 ram:24 disk_usage:200)

END()
