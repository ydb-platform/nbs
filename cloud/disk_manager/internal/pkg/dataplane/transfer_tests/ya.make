GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane)

SET_APPEND(RECIPE_ARGS --nbs-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_XTEST_SRCS(
    transfer_test.go
)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TAG(sb:ssd)

REQUIREMENTS(
    cpu:4
    ram:32
)

END()
