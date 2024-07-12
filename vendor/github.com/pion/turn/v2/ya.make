GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

# Test requires network connection

GO_SKIP_TESTS(TestClientWithSTUN)

SRCS(
    client.go
    errors.go
    lt_cred.go
    relay_address_generator_none.go
    relay_address_generator_range.go
    relay_address_generator_static.go
    server.go
    server_config.go
    stun_conn.go
)

GO_TEST_SRCS(
    client_test.go
    lt_cred_test.go
    server_test.go
)

END()

RECURSE(
    gotest
    internal
)
