GO_TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

INCLUDE(${ARCADIA_ROOT}/cloud/tasks/acceptance_tests/recipe/recipe.inc)

GO_XTEST_SRCS(
    tasks_acceptance_test.go
)

SIZE(LARGE)
TAG(
    ya:fat
    ya:force_sandbox
    ya:sandbox_coverage
)

REQUIREMENTS(
    cpu:4
    ram:16
)

END()

RECURSE(
    recipe
)
