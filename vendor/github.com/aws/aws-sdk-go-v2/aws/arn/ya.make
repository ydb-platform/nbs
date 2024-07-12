GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    arn.go
)

GO_TEST_SRCS(arn_test.go)

END()

RECURSE(
    gotest
)
