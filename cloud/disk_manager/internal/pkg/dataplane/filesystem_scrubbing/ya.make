GO_LIBRARY()

SRCS(
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
