OWNER(g:cloud-nbs)

GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET(RECIPE_ARGS --nemesis)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../snapshot_service_test/snapshot_service_test.go
)

END()
