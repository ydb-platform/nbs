GO_LIBRARY()

SRCS(
    backup_chunks_task.go
    backup_snapshot_task.go
    meta.go
    register.go
    slave.go
)

GO_TEST_SRCS(
    meta_test.go
    slave_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tasks_tests
    tests
)
