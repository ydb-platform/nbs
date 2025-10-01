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

GO_TEST_SRCS(
    collect_snapshots_task_test.go
    replicate_disk_task_test.go
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
    tasks_tests
    tests
    transfer_tests
)
