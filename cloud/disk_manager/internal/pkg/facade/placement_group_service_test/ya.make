GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    placement_group_service_test.go
)

END()
