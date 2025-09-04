GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    config.go
    conn.go
    context.go
    errors.go
    grpc_client_stream.go
    middleware.go
    pool.go
    state.go
)

GO_TEST_SRCS(
    conn_test.go
    errors_test.go
    grpc_client_conn_interface_mock_test.go
)

END()

RECURSE(
    gotest
)
