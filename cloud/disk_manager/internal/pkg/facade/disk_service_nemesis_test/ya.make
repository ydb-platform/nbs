GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --multiple-nbs)
SET_APPEND(RECIPE_ARGS --encryption)
SET_APPEND(RECIPE_ARGS --min-restart-period-sec 30)
SET_APPEND(RECIPE_ARGS --max-restart-period-sec 60)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

GO_XTEST_SRCS(
    ../disk_service_test/disk_relocation_test.go
    ../disk_service_test/disk_service_test.go
)

END()