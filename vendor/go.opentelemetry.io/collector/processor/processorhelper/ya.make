GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    logs.go
    metrics.go
    processor.go
    traces.go
)

GO_TEST_SRCS(
    logs_test.go
    metrics_test.go
    traces_test.go
)

END()

RECURSE(
    gotest
)
