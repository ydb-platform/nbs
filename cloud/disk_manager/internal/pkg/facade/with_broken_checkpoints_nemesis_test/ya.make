GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --disk-agent-count 3)
SET_APPEND(RECIPE_ARGS --retry-broken-disk-registry-based-disk-checkpoint)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../with_broken_checkpoints_test/with_broken_checkpoints_test.go
)

END()
