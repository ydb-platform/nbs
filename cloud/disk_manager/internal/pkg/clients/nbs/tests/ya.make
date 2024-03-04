GO_TEST_FOR(cloud/disk_manager/internal/pkg/clients/nbs)

SET_APPEND(RECIPE_ARGS --nbs-only)
SET_APPEND(RECIPE_ARGS --multiple-nbs)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_XTEST_SRCS(
    client_test.go
)

SIZE(MEDIUM)

END()
