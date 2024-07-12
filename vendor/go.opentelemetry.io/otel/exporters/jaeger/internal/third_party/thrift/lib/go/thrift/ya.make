GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    application_exception.go
    binary_protocol.go
    buffered_transport.go
    client.go
    compact_protocol.go
    configuration.go
    context.go
    debug_protocol.go
    deserializer.go
    exception.go
    framed_transport.go
    header_context.go
    header_protocol.go
    header_transport.go
    http_client.go
    http_transport.go
    iostream_transport.go
    json_protocol.go
    logger.go
    memory_buffer.go
    messagetype.go
    middleware.go
    multiplexed_protocol.go
    numeric.go
    pointerize.go
    processor_factory.go
    protocol.go
    protocol_exception.go
    protocol_factory.go
    response_helper.go
    rich_transport.go
    serializer.go
    server.go
    server_socket.go
    server_transport.go
    simple_json_protocol.go
    simple_server.go
    socket.go
    socket_conn.go
    ssl_server_socket.go
    ssl_socket.go
    transport.go
    transport_exception.go
    transport_factory.go
    type.go
    zlib_transport.go
)

IF (OS_LINUX)
    SRCS(
        socket_unix_conn.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        socket_unix_conn.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        socket_windows_conn.go
    )
ENDIF()

END()
