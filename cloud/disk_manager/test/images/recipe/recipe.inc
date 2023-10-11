SET(RECIPE_DEPS_LIST
    cloud/disk_manager/test/images/server
    cloud/disk_manager/test/images/recipe
    cloud/disk_manager/test/images/qcow2generator
)

DEPENDS(${RECIPE_DEPS_LIST})

DATA(
    arcadia/cloud/disk_manager/test/images/raw.img
    arcadia/cloud/disk_manager/test/images/qcow2.img
    arcadia/cloud/disk_manager/test/images/qcow2_other.img
    arcadia/cloud/disk_manager/test/images/vmdk.img
    arcadia/cloud/disk_manager/test/images/vmdk_stream_optimized.img
)

USE_RECIPE(cloud/disk_manager/test/images/recipe/recipe ${RECIPE_ARGS})
