GO_TEST_FOR(cloud/disk_manager/internal/pkg/services/pools/storage)

# These storage tests need only YDB, not NBS, NFS, DM or S3 services.
DEPENDS(cloud/disk_manager/internal/pkg/services/pools/storage/tests/ydb_recipe)

IF (OPENSOURCE)
    DEPENDS(cloud/storage/core/tools/testing/ydb/bin)
ELSE()
    DEPENDS(contrib/ydb/apps/ydbd)
ENDIF()

USE_RECIPE(cloud/disk_manager/internal/pkg/services/pools/storage/tests/ydb_recipe/ydb_recipe)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
