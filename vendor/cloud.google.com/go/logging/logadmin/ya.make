GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    logadmin.go
    metrics.go
    resources.go
    sinks.go
)

GO_TEST_SRCS(
    logadmin_test.go
    metrics_test.go
    resources_test.go
    sinks_test.go
)

GO_XTEST_SRCS(
    example_entry_iterator_test.go
    example_metric_iterator_test.go
    example_paging_test.go
    example_resource_iterator_test.go
    example_sink_iterator_test.go
    examples_test.go
)

END()

RECURSE(
    gotest
)
