GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

# Strict regression: deliberately not in required RECURSE_FOR_TESTS/PR CI.
DEPENDS(cloud/disk_manager/test/mocks/s3_fault_proxy/cmd/s3_fault_proxy)
SET_APPEND(RECIPE_ARGS --backup --backup-fault-proxy --backup-task-max-retriable-errors 3)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../snapshot_service_backup_test/snapshot_service_backup_test.go
    snapshot_service_backup_retry_exhaustion_test.go
)

END()
