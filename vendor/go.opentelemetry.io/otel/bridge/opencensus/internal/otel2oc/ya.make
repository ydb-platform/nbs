GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    span_context.go
)

GO_TEST_SRCS(span_context_test.go)

END()

RECURSE(
    gotest
)
