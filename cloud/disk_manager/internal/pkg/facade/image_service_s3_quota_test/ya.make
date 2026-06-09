GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SPLIT_FACTOR(1)

SET_APPEND(RECIPE_ARGS --image-s3-storage-class STANDARD_IA)
SET_APPEND(RECIPE_ARGS --s3-quota STANDARD_IA:1)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    image_service_s3_quota_test.go
)

END()
