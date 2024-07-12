GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

# Test requires IPv4

GO_SKIP_TESTS(TestValidCommunication)

SRCS(
    config.go
    conn.go
    errors.go
    mdns.go
)

GO_TEST_SRCS(conn_test.go)

END()

RECURSE(
    gotest
)
