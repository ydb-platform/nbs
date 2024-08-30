SET(RECIPE_DEPS_LIST
    cloud/disk_manager/test/images/server
    cloud/disk_manager/test/images/recipe
    cloud/disk_manager/test/images/resources
    cloud/disk_manager/test/images/qcow2generator
)

DEPENDS(${RECIPE_DEPS_LIST})

DATA(
    arcadia/cloud/disk_manager/test/images/recipe/data
)

USE_RECIPE(cloud/disk_manager/test/images/recipe/recipe ${RECIPE_ARGS})
