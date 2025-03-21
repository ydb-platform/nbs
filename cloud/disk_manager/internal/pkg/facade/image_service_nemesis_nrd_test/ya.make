GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --disk-agent-count 3)
SET_APPEND(RECIPE_ARGS --retry-broken-drbased-disk-checkpoint)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../image_service_nrd_test/image_service_nrd_test.go
)

END()
