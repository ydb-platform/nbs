GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/url)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

GO_XTEST_SRCS(
    qcow2_test.go
)

SIZE(MEDIUM)

DEPENDS(
    cloud/disk_manager/test/images/resources
)

END()
