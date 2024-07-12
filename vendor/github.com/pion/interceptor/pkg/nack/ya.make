GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    errors.go
    generator_interceptor.go
    generator_option.go
    nack.go
    receive_log.go
    responder_interceptor.go
    responder_option.go
    retainable_packet.go
    send_buffer.go
)

GO_TEST_SRCS(
    generator_interceptor_test.go
    receive_log_test.go
    responder_interceptor_test.go
    send_buffer_test.go
)

END()

RECURSE(
    gotest
)
