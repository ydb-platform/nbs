GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    endpoints.go
)

GO_TEST_SRCS(endpoints_test.go)

END()

RECURSE(
    awsrulesfn
    gotest
)
