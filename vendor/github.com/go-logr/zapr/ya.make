GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    zapr.go
)

GO_XTEST_SRCS(
    example_test.go
    zapr_test.go
    zapr_wrapper_test.go
)

END()

RECURSE(
    example
    gotest
    internal
)
