OWNER(g:cloud-nbs)

GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/url)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

GO_XTEST_SRCS(
    vmdk_test.go
)

DATA(arcadia/cloud/disk_manager/internal/pkg/dataplane/url/vmdk/tests/data)

SIZE(MEDIUM)

DATA(
    sbr://4709742882=vmdk_images
)

END()
