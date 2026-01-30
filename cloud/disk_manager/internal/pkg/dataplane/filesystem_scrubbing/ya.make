GO_LIBRARY()

SRCS(
    scrub_filesystem_task.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    tests
)
