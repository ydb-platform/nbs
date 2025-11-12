GO_LIBRARY()

SRCS(
    create_filesystem_snapshot_task.go
    delete_filesystem_snapshot_task.go
    interface.go
    register.go
    service.go
)

END()

RECURSE_FOR_TESTS(
    mocks
)
