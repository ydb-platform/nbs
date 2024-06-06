GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    attribute.go
    error.go
    metricdata.go
)

GO_TEST_SRCS(
    attribute_test.go
    error_test.go
    metricdata_test.go
)

END()

RECURSE(
    gotest
)
