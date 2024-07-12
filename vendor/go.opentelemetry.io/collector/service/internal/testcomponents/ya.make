GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    example_connector.go
    example_exporter.go
    example_processor.go
    example_receiver.go
    example_router.go
    stateful_component.go
)

GO_TEST_SRCS(
    example_connector_test.go
    example_exporter_test.go
    example_processor_test.go
    example_receiver_test.go
    example_router_test.go
)

END()

RECURSE(
    gotest
)
