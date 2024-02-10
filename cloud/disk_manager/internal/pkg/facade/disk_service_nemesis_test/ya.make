GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --multiple-nbs)
SET_APPEND(RECIPE_ARGS --encryption)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../disk_service_test/disk_relocation_test.go
    ../disk_service_test/disk_service_test.go
)

END()
