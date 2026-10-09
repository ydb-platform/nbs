GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane)

# Explicit opt-in regressions for unresolved product defects. Do not add to
# RECURSE_FOR_TESTS or the required PR matrix until those defects are fixed.
SET_APPEND(RECIPE_ARGS --nbs-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_TEST_SRCS(
    ../backup_chunks_task_test.go
    ../backup_faults_test.go
    ../backup_regressions_test.go
    ../backup_snapshot_data_task_test.go
    ../delete_snapshot_data_task_test.go
)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
