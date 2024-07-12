GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    context.go
    doc.go
    jaeger_propagator.go
    version.go
)

GO_TEST_SRCS(
    context_test.go
    jaeger_propagator_test.go
)

GO_XTEST_SRCS(
    jaeger_data_test.go
    jaeger_example_test.go
    jaeger_integration_test.go
)

END()

RECURSE(
    gotest
)
