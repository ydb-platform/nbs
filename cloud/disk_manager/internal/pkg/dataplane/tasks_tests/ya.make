GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane)

SET_APPEND(RECIPE_ARGS --nbs-only)
SET_APPEND(RECIPE_ARGS --encryption)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_TEST_SRCS(
    ../backup_chunks_task_test.go
    ../backup_snapshot_data_task_test.go
    ../delete_snapshot_data_task_test.go
    ../transfer_from_backup_to_disk_task_nbs_test.go
)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
