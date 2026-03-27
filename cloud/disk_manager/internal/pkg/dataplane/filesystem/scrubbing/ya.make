GO_LIBRARY()

SRCS(
    register.go
    regular_scrub_filesystems_task.go
    scrub_filesystem_task.go
)

GO_TEST_SRCS(
    scrub_filesystem_task_test.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    tests
)
