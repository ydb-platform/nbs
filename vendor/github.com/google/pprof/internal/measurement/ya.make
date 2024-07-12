GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    measurement.go
)

GO_TEST_SRCS(measurement_test.go)

END()

RECURSE(
    gotest
)
