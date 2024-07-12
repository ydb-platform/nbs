GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    metrics.go
)

GO_TEST_SRCS(metrics_test.go)

END()

RECURSE(
    emf
    gotest
    middleware
    publisher
    readcloserwithmetrics
    testutils
)
