DEPENDS(
    cloud/tasks/acceptance_tests/recipe
    cloud/tasks/acceptance_tests/recipe/init-db
    cloud/tasks/acceptance_tests/recipe/node
    cloud/disk_manager/test/nemesis
)

USE_RECIPE(cloud/tasks/acceptance_tests/recipe/recipe)
