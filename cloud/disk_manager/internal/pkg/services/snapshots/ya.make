GO_LIBRARY()

SRCS(
    backup_snapshot_task.go
    clear_deleted_snapshots_task.go
    create_snapshot_from_disk_task.go
    delete_snapshot_task.go
    interface.go
    register.go
    service.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    mocks
    tasks_tests
)
