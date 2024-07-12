GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    b3_config.go
    b3_propagator.go
    context.go
    doc.go
    version.go
)

GO_TEST_SRCS(
    b3_propagator_test.go
    context_test.go
)

GO_XTEST_SRCS(
    b3_benchmark_test.go
    b3_data_test.go
    b3_example_test.go
    b3_integration_test.go
)

END()

RECURSE(
    gotest
)
