GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    context.go
    current_file_line.go
    grpclogger.go
    logger.go
    manytimes.go
    test_condition.go
    waiters.go
)

GO_TEST_SRCS(current_file_line_test.go)

END()

RECURSE(
    gotest
)
