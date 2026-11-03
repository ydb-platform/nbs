GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --backup --backup-fault-proxy --nemesis --controlled-nemesis)
SET_APPEND(RECIPE_ARGS --backup-s3-call-timeout-sec 30)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    snapshot_service_backup_crash_test.go
)

END()
