GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    metric.go
)

GO_TEST_SRCS(metric_test.go)

END()

RECURSE(
    gotest
)
