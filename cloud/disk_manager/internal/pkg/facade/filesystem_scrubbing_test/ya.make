GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --filesystem-dataplane-enabled)
SET_APPEND(RECIPE_ARGS --enable-list-nodes-logging)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    filesystem_scrubbing_test.go
    filesystem_scrubbing_metrics_test.go
)

END()
