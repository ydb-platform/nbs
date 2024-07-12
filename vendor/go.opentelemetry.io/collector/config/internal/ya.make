GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    warning.go
)

GO_TEST_SRCS(warning_test.go)

END()

RECURSE(
    gotest
)
