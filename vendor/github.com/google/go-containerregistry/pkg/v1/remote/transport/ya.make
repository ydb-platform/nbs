GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    basic.go
    bearer.go
    doc.go
    error.go
    logger.go
    ping.go
    retry.go
    schemer.go
    scope.go
    transport.go
    useragent.go
)

GO_TEST_SRCS(
    basic_test.go
    bearer_test.go
    error_test.go
    logger_test.go
    ping_test.go
    retry_test.go
    transport_test.go
)

END()

RECURSE(
    gotest
)
