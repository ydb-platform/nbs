GO_LIBRARY()

SRCS(
    clear_deleted_filesystem_backups_task.go
    create_filesystem_backup_from_filesystem_task.go
    delete_filesystem_backup_task.go
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
)
