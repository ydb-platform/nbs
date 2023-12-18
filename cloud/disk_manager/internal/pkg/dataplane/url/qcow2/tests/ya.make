GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/url)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

GO_XTEST_SRCS(
    qcow2_test.go
)

SIZE(MEDIUM)

DATA(
    sbr://2951476475=qcow2_images
    sbr://3064742393=qcow2_images
)

END()
