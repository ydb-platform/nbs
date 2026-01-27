GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --with-filestore-cells)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../filesystem_service_test/filesystem_service_test.go
)

END()
