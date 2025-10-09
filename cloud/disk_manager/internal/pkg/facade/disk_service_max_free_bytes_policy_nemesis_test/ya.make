GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --multiple-nbs)
SET_APPEND(RECIPE_ARGS --min-restart-period-sec 30)
SET_APPEND(RECIPE_ARGS --max-restart-period-sec 60)
SET_APPEND(RECIPE_ARGS --creation-and-deletion-allowed-only-for-disks-with-id-prefix "Test")
SET_APPEND(RECIPE_ARGS --disable-disk-registry-based-disks)
SET_APPEND(RECIPE_ARGS --cell-selection-policy "MAX_FREE_BYTES")
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../disk_service_max_free_bytes_policy_test/disk_service_max_free_bytes_policy_test.go
)

END()
