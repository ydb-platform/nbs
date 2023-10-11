OWNER(g:cloud-nbs)

GO_TEST()

SET(RECIPE_ARGS kikimr)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe/recipe.inc)

GO_XTEST_SRCS(
    tasks_acceptance_test.go
)

SIZE(LARGE)
TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)

REQUIREMENTS(
    cpu:4
    ram:16
)

END()

RECURSE(
    recipe
)
