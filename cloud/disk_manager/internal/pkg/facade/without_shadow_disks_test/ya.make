GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --without-shadow-disks)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    without_shadow_disks_test.go
)

END()
