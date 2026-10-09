GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

# Random-restart smoke coverage, not a deterministic PUT/checkpoint crash test.
SET_APPEND(RECIPE_ARGS --backup --nemesis)
SET_APPEND(RECIPE_ARGS --min-restart-period-sec 5 --max-restart-period-sec 10)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../snapshot_service_backup_test/snapshot_service_backup_test.go
)

END()
