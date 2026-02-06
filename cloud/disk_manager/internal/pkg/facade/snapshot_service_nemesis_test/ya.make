GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

# To configure NBS with Root KMS support, Fake Root KMS must be started first
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/fake-root-kms/recipe.inc)


SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --disk-agent-count 5)
SET_APPEND(RECIPE_ARGS --encryption)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../snapshot_service_test/snapshot_service_test.go
)

END()
