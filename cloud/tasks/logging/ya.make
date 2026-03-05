GO_LIBRARY()

SRCS(
    context_logger.go
    fields.go
    interface.go
    journald_logger.go
    logger.go
    stderr_logger.go
)

GO_TEST_SRCS(
    logger_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    journald_tests
    tests
)
