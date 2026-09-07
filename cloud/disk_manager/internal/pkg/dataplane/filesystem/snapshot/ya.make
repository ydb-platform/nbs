GO_LIBRARY()

SRCS(
    collect_filesystem_snapshots_task.go
    delete_filesystem_snapshot_data_task.go
    delete_filesystem_snapshot_task.go
    hardlink_batch_restorer.go
    register.go
    create_snapshot_from_filesystem_task.go
    transfer_from_snapshot_to_filesystem_task.go
)

GO_TEST_SRCS(
    hardlink_batch_restorer_test.go
    transfer_task_test.go
)

END()

RECURSE(
    config
    protos
    storage
)

RECURSE_FOR_TESTS(
    tests
)
