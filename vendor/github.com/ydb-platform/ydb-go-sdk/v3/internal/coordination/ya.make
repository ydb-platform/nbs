GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    session.go
)

GO_TEST_SRCS(
    client_test.go
    grpc_client_mock_test.go
)

END()

RECURSE(
    config
    conversation
    gotest
)
