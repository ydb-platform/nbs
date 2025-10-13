GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --disk-agent-count 3)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../snapshot_service_test/snapshot_service_test.go
)

END()
