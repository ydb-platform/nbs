GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_XTEST_SRCS(
    facade_test.go
)

SIZE(MEDIUM)

END()
