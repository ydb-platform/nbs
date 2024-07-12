GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    audit_logger.go
)

GO_XTEST_SRCS(
    # audit_logging_test.go
)

END()

RECURSE(
    gotest
    stdout
)
