GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    read_closer_with_metrics.go
)

GO_TEST_SRCS(read_closer_with_metrics_test.go)

END()

RECURSE(
    gotest
)
