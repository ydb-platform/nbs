GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    client.go
    discover.go
    error.go
    grpc_broker.go
    grpc_client.go
    grpc_controller.go
    grpc_server.go
    grpc_stdio.go
    log_entry.go
    mtls.go
    mux_broker.go
    plugin.go
    process.go
    protocol.go
    rpc_client.go
    rpc_server.go
    server.go
    server_mux.go
    stream.go
    testing.go
)

GO_TEST_SRCS(
    # client_test.go
    error_test.go
    grpc_client_test.go
    plugin_test.go
    rpc_client_test.go
    server_test.go
)

IF (OS_LINUX)
    SRCS(
        notes_unix.go
        process_posix.go
    )

    GO_TEST_SRCS(client_posix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        notes_unix.go
        process_posix.go
    )

    GO_TEST_SRCS(client_posix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        notes_windows.go
        process_windows.go
    )
ENDIF()

END()

RECURSE(
    examples
    gotest
    internal
    test
)
