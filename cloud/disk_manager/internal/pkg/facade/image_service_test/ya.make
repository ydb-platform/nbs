GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

# To configure NBS with Root KMS support, Fake Root KMS must be started first
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/fake-root-kms/recipe.inc)

SET_APPEND(RECIPE_ARGS --generate-vmdk-image)
SET_APPEND(RECIPE_ARGS --generate-big-raw-images)
SET_APPEND(RECIPE_ARGS --encryption)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    image_service_test.go
)

DEPENDS(
    cloud/disk_manager/test/images/resources
)

REQUIREMENTS(
    container:4915490540  # image with qemu-utils (qemu-img in particular)
)

END()
