DEPENDS(
    cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe
    cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe/init-db
    cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe/node
    cloud/disk_manager/test/nemesis
)

USE_RECIPE(cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe/recipe)
