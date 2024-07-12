GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    error_utils.go
)

GO_TEST_SRCS(error_utils_test.go)

END()

RECURSE(
    gotest
)
