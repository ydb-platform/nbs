GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    cipher_suite.go
    errors.go
    handshake.go
    header.go
    message_certificate.go
    message_certificate_request.go
    message_certificate_verify.go
    message_client_hello.go
    message_client_key_exchange.go
    message_finished.go
    message_hello_verify_request.go
    message_server_hello.go
    message_server_hello_done.go
    message_server_key_exchange.go
    random.go
)

GO_TEST_SRCS(
    cipher_suite_test.go
    fuzz_test.go
    message_certificate_request_test.go
    message_certificate_test.go
    message_certificate_verify_test.go
    message_client_hello_test.go
    message_client_key_exchange_test.go
    message_finished_test.go
    message_hello_verify_request_test.go
    message_server_hello_done_test.go
    message_server_hello_test.go
    message_server_key_exchange_test.go
)

END()

RECURSE(
    gotest
)
