GO_LIBRARY()

SRCS(
    clear_deleted_filesystems_task.go
    create_external_filesystem_task.go
    create_filesystem_task.go
    delete_external_filesystem_task.go
    delete_filesystem_task.go
    interface.go
    register.go
    resize_filesystem_task.go
    service.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    mocks
    tests
    ut
)
