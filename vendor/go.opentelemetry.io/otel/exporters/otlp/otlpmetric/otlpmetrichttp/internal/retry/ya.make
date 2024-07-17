GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    retry.go
)

GO_TEST_SRCS(retry_test.go)

END()

RECURSE(
    gotest
)
