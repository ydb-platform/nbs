OWNER(g:cloud-nbs)

GO_TEST()

SET(RECIPE_ARGS kikimr)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_XTEST_SRCS(
    tasks_test.go
)

SIZE(MEDIUM)

END()
