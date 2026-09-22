GO_LIBRARY()

SRCS(
    clear_deleted_snapshots_task.go
    create_snapshot_from_disk_task.go
    delete_snapshot_task.go
    interface.go
    register.go
    service.go
    staggering.go
)

GO_TEST_SRCS(
    staggering_test.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
