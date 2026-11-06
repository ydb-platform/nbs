GO_LIBRARY()

SRCS(
    backup_snapshot_task.go
    clear_deleted_snapshots_task.go
    create_snapshot_from_disk_task.go
    delete_snapshot_task.go
    interface.go
    register.go
    schedule_backup_snapshot_tasks.go
    service.go
)

GO_TEST_SRCS(
    backup_control_test.go
    backup_registration_test.go
    schedule_backup_snapshot_tasks_test.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    tests
    mocks
    tasks_tests
)
