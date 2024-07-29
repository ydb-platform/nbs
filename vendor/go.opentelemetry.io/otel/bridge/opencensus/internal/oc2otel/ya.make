GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    attributes.go
    span_context.go
    tracer_start_options.go
)

GO_TEST_SRCS(
    attributes_test.go
    span_context_test.go
    tracer_start_options_test.go
)

END()

RECURSE(
    gotest
)
