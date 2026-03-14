GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --filesystem-dataplane-enabled)
SET_APPEND(RECIPE_ARGS --enable-list-nodes-logging)
SET_APPEND(RECIPE_ARGS --allow-filestore-force-destroy)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

DEPENDS(
    cloud/filestore/apps/client
)

GO_XTEST_SRCS(
    filesystem_traversal_test.go
)

END()
