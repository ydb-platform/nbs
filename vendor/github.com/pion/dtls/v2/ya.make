GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    certificate.go
    cipher_suite.go
    cipher_suite_go114.go
    compression_method.go
    config.go
    conn.go
    crypto.go
    dtls.go
    errors.go
    errors_errno.go
    flight.go
    flight0handler.go
    flight1handler.go
    flight2handler.go
    flight3handler.go
    flight4bhandler.go
    flight4handler.go
    flight5bhandler.go
    flight5handler.go
    flight6handler.go
    flighthandler.go
    fragment_buffer.go
    handshake_cache.go
    handshaker.go
    listener.go
    packet.go
    resume.go
    session.go
    srtp_protection_profile.go
    state.go
    util.go
)

GO_TEST_SRCS(
    bench_test.go
    certificate_test.go
    cipher_suite_go114_test.go
    cipher_suite_test.go
    config_test.go
    conn_go_test.go
    conn_test.go
    crypto_test.go
    errors_errno_test.go
    errors_test.go
    flight4handler_test.go
    fragment_buffer_test.go
    handshake_cache_test.go
    handshake_test.go
    handshaker_test.go
    nettest_test.go
    replayprotection_test.go
    resume_test.go
)

END()

RECURSE(
    gotest
    internal
    pkg
)
