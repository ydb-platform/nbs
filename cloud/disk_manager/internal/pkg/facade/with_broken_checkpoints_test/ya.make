GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --disk-agent-count 3)
SET_APPEND(RECIPE_ARGS --retry-broken-disk-registry-based-disk-checkpoint)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

# We want each test to run in separate chunk in order to avoid flaps.
# TODO: recodsider this after https://github.com/ydb-platform/nbs/issues/3363.
SPLIT_FACTOR(5)

GO_XTEST_SRCS(
    with_broken_checkpoints_test.go
)

END()
