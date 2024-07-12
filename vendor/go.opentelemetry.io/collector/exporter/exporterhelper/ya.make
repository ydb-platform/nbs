GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    common.go
    constants.go
    doc.go
    logs.go
    metrics.go
    obsreport.go
    queued_retry.go
    request.go
    traces.go
)

GO_TEST_SRCS(
    common_test.go
    logs_test.go
    metrics_test.go
    obsreport_test.go
    queued_retry_test.go
    request_test.go
    traces_test.go
)

END()

RECURSE(
    gotest
    internal
)
