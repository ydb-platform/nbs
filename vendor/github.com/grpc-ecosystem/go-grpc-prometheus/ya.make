GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client.go
    client_metrics.go
    client_reporter.go
    metric_options.go
    server.go
    server_metrics.go
    server_reporter.go
    util.go
)

GO_TEST_SRCS(
    # client_test.go
    # server_test.go
)

END()

RECURSE(
    examples
    gotest
)
