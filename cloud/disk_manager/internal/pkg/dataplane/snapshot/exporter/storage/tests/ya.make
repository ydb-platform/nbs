GO_TEST()

GO_TEST_SRCS(
    storage_test.go
)

DEPENDS(
    cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter/storage/tests/recipe
    cloud/storage/core/tools/testing/ydb/bin
)

USE_RECIPE(cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter/storage/tests/recipe/recipe)

REQUIREMENTS(ram:16)

SIZE(MEDIUM)

END()
