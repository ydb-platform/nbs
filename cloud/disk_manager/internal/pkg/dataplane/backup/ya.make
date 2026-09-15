GO_LIBRARY()

SRCS(
    backup_chunks_task.go
    backup_snapshot_task.go
    keys.go
    meta.go
    register.go
)

GO_TEST_SRCS(
    keys_test.go
    meta_test.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    tasks_tests
    tests
)
