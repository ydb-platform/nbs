GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    config.go
    conn.go
    context.go
    error.go
    grpc_client_stream.go
    middleware.go
    pool.go
    state.go
)

GO_TEST_SRCS(error_test.go)

END()

RECURSE(
    gotest
)
