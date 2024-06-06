GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    doc.go
    metric.go
    trace.go
    version.go
)

GO_TEST_SRCS(
    config_test.go
    metric_test.go
    trace_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    internal
)
