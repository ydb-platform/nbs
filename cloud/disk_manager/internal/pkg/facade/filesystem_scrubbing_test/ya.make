GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --filesystem-dataplane-enabled)
SET_APPEND(RECIPE_ARGS --regular-filesystem-scrubbing-config=cloud/disk_manager/internal/pkg/facade/filesystem_scrubbing_test/scrubbing_config.txt)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)
SPLIT_FACTOR(1)

GO_XTEST_SRCS(
    filesystem_scrubbing_test.go
    filesystem_scrubbing_metrics_test.go
)


DATA(
    arcadia/cloud/disk_manager/internal/pkg/facade/filesystem_scrubbing_test/scrubbing_config.txt
)

END()
