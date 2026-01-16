GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)
SET_APPEND(RECIPE_ARGS --with-filestore-cells)
GO_XTEST_SRCS(
    filesystem_service_test.go
)

END()
