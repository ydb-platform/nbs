GO_LIBRARY()

SRCS(
    collect_snapshot_metrics_task.go
    collect_snapshots_task.go
    consts.go
    create_dr_based_disk_checkpoint_task.go
    create_snapshot_from_disk_task.go
    create_snapshot_from_legacy_snapshot_task.go
    create_snapshot_from_snapshot_task.go
    create_snapshot_from_url_task.go
    delete_disk_from_incremental.go
    delete_snapshot_data_task.go
    delete_snapshot_task.go
    migrate_snapshot.go
    migrate_snapshot_database_task.go
    register.go
    replicate_disk_task.go
    transfer_from_disk_to_disk_task.go
    transfer_from_snapshot_to_disk_task.go
)

SET_APPEND(RECIPE_ARGS --nbs-only)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_TEST_SRCS(
    collect_snapshots_task_test.go
    delete_snapshot_data_task_test.go
    replicate_disk_task_test.go
)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TAG(sb:ssd)

REQUIREMENTS(
    cpu:4
    ram:32
)

END()

RECURSE(
    common
    config
    nbs
    protos
    snapshot
    test
    url
)

RECURSE_FOR_TESTS(
    tests
    transfer_tests
)
