GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    logger.go
)

GO_XTEST_SRCS(logger_test.go)

END()

RECURSE(
    gotest
)
