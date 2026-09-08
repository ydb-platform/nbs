GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --snapshot-chunk-size 8388608)
SET_APPEND(RECIPE_ARGS --image-chunk-size 8388608)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    chunk_size_test.go
    scheduled_tasks_test.go
)

END()
