GO_TEST_FOR(cloud/disk_manager/internal/pkg/facade)

SET_APPEND(RECIPE_ARGS --nemesis)
SET_APPEND(RECIPE_ARGS --allow-filestore-force-destroy)
SET_APPEND(RECIPE_ARGS --filesystem-dataplane-enabled)
SET_APPEND(RECIPE_ARGS --list-nodes-max-bytes=10000)
SET_APPEND(RECIPE_ARGS --regular-filesystem-scrubbing-config=cloud/disk_manager/internal/pkg/facade/filesystem_scrubbing_test/scrubbing_config.txt)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/internal/pkg/facade/testcommon/common.inc)

GO_XTEST_SRCS(
    ../filesystem_scrubbing_test/filesystem_scrubbing_test.go
)

DATA(
    arcadia/cloud/disk_manager/internal/pkg/facade/filesystem_scrubbing_test/scrubbing_config.txt
)

END()
