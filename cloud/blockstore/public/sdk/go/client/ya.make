GO_LIBRARY()

SRCS(
    blocks.go
    client.go
    credentials.go
    discovery.go
    durable.go
    error.go
    error_codes.go
    grpc.go
    iface.go
    log.go
    request.go
    response.go
    safe_client.go
    session.go
    test_client.go
    time.go
    type.go
    uuid.go
)

GO_TEST_SRCS(
    blocks_test.go
    client_test.go
    discovery_test.go
    durable_test.go
    error_test.go
    grpc_test.go
    log_test.go
    session_test.go
)

END()

RECURSE_FOR_TESTS(ut)
