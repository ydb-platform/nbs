GO_LIBRARY(filestore-public-sdk-go-client)

SRCS(
    client.go
    credentials.go
    durable.go
    error.go
    error_codes.go
    grpc.go
    iface.go
    log.go
    request.go
    response.go
    test_client.go
    time.go
    type.go
    uuid.go
)

GO_TEST_SRCS(
    durable_test.go
    grpc_test.go
    log_test.go
)

END()

RECURSE_FOR_TESTS(ut)
