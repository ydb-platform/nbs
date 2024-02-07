GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET(RECIPE_ARGS --nemesis)
SET(RECIPE_ARGS --min-restart-period-sec 30)
SET(RECIPE_ARGS --max-restart-period-sec 60)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../private_service_test/private_service_test.go
)

END()
