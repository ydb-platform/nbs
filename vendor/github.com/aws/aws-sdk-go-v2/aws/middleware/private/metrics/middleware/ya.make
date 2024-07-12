GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    configuration.go
    endpoint_resolution_end.go
    endpoint_resolution_start.go
    http.go
    identity.go
    metric_collection.go
    request.go
    request_attempt.go
    signing.go
    stack_deserialize_end.go
    stack_deserialize_start.go
    stack_serialize_end.go
    stack_serialize_start.go
    transport.go
    user_agent.go
    wrap_data_stream.go
)

GO_TEST_SRCS(
    endpoint_resolution_end_test.go
    endpoint_resolution_start_test.go
    http_test.go
    metric_collection_test.go
    request_attempt_test.go
    request_test.go
    stack_deserialize_end_test.go
    stack_deserialize_start_test.go
    stack_serialize_end_test.go
    stack_serialize_start_test.go
    transport_test.go
    wrap_data_streams_test.go
)

END()

RECURSE(
    gotest
)
