GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)
SPLIT_FACTOR(2)

GO_XTEST_SRCS(
    ../placement_group_service_test/placement_group_service_test.go
)

END()
