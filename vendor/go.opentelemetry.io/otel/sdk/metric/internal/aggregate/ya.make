GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aggregate.go
    doc.go
    exponential_histogram.go
    histogram.go
    lastvalue.go
    limit.go
    sum.go
)

GO_TEST_SRCS(
    aggregate_test.go
    exponential_histogram_test.go
    histogram_test.go
    lastvalue_test.go
    limit_test.go
    sum_test.go
)

END()

RECURSE(
    gotest
)
