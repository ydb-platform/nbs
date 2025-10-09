GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

# To configure NBS with Root KMS support, Fake Root KMS must be started first
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/fake-root-kms/recipe.inc)

SET_APPEND(RECIPE_ARGS --multiple-nbs)
SET_APPEND(RECIPE_ARGS --encryption)
SET_APPEND(RECIPE_ARGS --creation-and-deletion-allowed-only-for-disks-with-id-prefix "Test")
SET_APPEND(RECIPE_ARGS --disable-disk-registry-based-disks)
SET_APPEND(RECIPE_ARGS --cell-selection-policy "MAX_FREE_BYTES")
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../disk_service_max_free_bytes_policy_test/disk_service_max_free_bytes_policy_test.go
)

END()
