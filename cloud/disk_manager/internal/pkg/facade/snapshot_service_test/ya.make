GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --creation-and-deletion-allowed-only-for-disks-with-id-prefix "Test")
SET_APPEND(RECIPE_ARGS --disk-agent-count 3)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    common_test.go
    snapshot_service_cells_test.go
    snapshot_service_test.go
)

END()
