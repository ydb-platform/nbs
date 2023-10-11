OWNER(g:cloud-nbs)

GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET(RECIPE_ARGS nemesis)
SET_APPEND(RECIPE_ARGS generate-vmdk-image)
SET_APPEND(RECIPE_ARGS encryption)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../image_service_test/image_service_test.go
)

DATA(
    sbr://2951476475=qcow2_images
)

REQUIREMENTS(
    container:4915490540  # image with qemu-utils (qemu-img in particular)
)

END()
